
package io.transwarp.graph.algorithms

import io.transwarp.graph._
import io.transwarp.graph.algorithms.GraphCommonUtils.{CountDoubleAggrBuffer, DoubleDegreeAggrBuffer, DoubleEdgeMergeAggrBuffer, DistinctAggrBuffer, log}
import io.transwarp.graph.algorithms.NormType.NormType
import io.transwarp.graph.shuffle.GraphKey
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, Description}
import org.apache.hadoop.hive.serde2.io.{ShortWritable, DoubleWritable}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io._

/**
  * Created by wzp on 16-6-27.
  */
@Description(name = "graph_authority_score",
  value = "_FUNC_(src, dst, <iteration(Int) = 20>, <norm(String) = 'standard' | 'max'>)\nresult column name: (vertex, score)")
class GraphAuthorityScoreOperator extends GraphOperator {

  @transient var nodeType: PrimitiveCategory = _
  @transient var iteration: Int = 20
  @transient var norm: String = "standard"

  // Define input and ouput column shema
  override def operatorSchema(inputOIs: Array[PrimitiveObjectInspector]): Array[GraphSchema] = {
    if (inputOIs.length < 2 || inputOIs.length > 4) {
      throw new UDFArgumentException(ErrorMsg.ERROR_11301, "graph_authority_score")
    }

    if (inputOIs(0).getPrimitiveCategory != inputOIs(1).getPrimitiveCategory) {
      throw new UDFArgumentException(ErrorMsg.ERROR_11300, "Type for vertices must be equal.")
    }

    nodeType = inputOIs(0).getPrimitiveCategory
    iteration = getTypedConstant(GraphInt, inputOIs, 2, 20)
    norm = getTypedConstant(GraphString, inputOIs, 3, "standard")

    if ((!norm.equals("standard") && !norm.equals("max")) || iteration < 1) {
      throw new UDFArgumentException(ErrorMsg.ERROR_11300, "Invalid parameter.")
    }

    Array(GraphSchema("vertex", nodeType), GraphSchema("score", GraphDouble))
  }

  override def operatorExecute(rdd: GraphRDD): GraphRDD = {
    val rowOI = rdd.rowOI
    val nodeType = this.nodeType
    val filterSeq = Seq(0, 1)

    val nonNullEdgeRdd = rdd.filter(row => GraphRDD.isNotNull(row, rowOI, filterSeq))

    val edgesRdd = nonNullEdgeRdd.makeKVGraphRdd(Seq(0, 1))
      .mapKVPartitionsWithIndex(
        (index: Int, part: Iterator[_]) => {
          part.map {
            case (k: GraphKey, row) => {
              val fields = rowOI.getAllStructFieldRefs

              val srcField = fields.get(0)
              val srcNode = rowOI.getStructFieldData(row, srcField)
              val dstField = fields.get(1)
              val dstNode = rowOI.getStructFieldData(row, dstField)

              (k, Array(srcNode, dstNode))
            }
          }
        },
        p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(nodeType, nodeType))
      )
      .groupByKey(nonNullEdgeRdd.getReducerSize(nonNullEdgeRdd.partitions.length), () => new DistinctAggrBuffer())
      .makeGraphRdd()
      .makeKVGraphRdd(Seq(0))

    edgesRdd.rePartition()
    val filnalEdgeRDD =  edgesRdd.makeGraphRdd()

    val vertexRdd = filnalEdgeRDD.mapPartitionsWithIndex((index, part) => {
      part.flatMap {
        case (row: Array[Any]) => Seq(Array(row(0), new DoubleWritable(1), new DoubleWritable(1), new DoubleWritable(1), new DoubleWritable(1)),
          Array(row(1), new DoubleWritable(1), new DoubleWritable(1), new DoubleWritable(1), new DoubleWritable(1)))
        //authority, hub, deltaAuth, deltaHub
      }
    },
      p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(
        Seq(nodeType, PrimitiveCategory.DOUBLE, PrimitiveCategory.DOUBLE, PrimitiveCategory.DOUBLE, PrimitiveCategory.DOUBLE)
      )
    ).makeKVGraphRdd(Seq(0))
      .groupByKey(edgesRdd.partitions.length, () => new DistinctAggrBuffer())

    val normType = norm match {
      case "standard" => NormType.STANDARD
      case "max" => NormType.MAX
    }

    GraphAuthorityScoreOperator.runAuthorityAndHub(filnalEdgeRDD, vertexRdd, nodeType, iteration, true, normType)
  }
}

object NormType extends Enumeration {
  type NormType = Value
  val STANDARD, MAX = Value
}

object GraphAuthorityScoreOperator {

  class MaxDoubleAggrBuffer extends GraphAggrBuffer {
    var size: Double = 0

    override def merge(o: Any): Unit = {
      val newSize = o.asInstanceOf[Array[Any]](0).asInstanceOf[DoubleWritable].get()
      if (newSize > size) {
        size = newSize
      }
    }

    override def finishedAndGetResult(): Any = Array(new DoubleWritable(size))
  }

  def runAuthorityAndHub(edgesRdd: GraphRDD,
                         vertexRdd1: KVGraphRDD,
                         nodeType: PrimitiveCategory,
                         iteration: Int,
                         authority: Boolean,
                         normType: NormType): GraphRDD = {
    var k = 0
    var delta = true
    var vertexRdd = vertexRdd1

    while (delta && k < iteration) {
      log.info(s"Round ${k} in AuthorityAndHub")
      val u1 = edgesRdd.makeKVGraphRdd(Seq(0))
        .join(Seq(vertexRdd),
          (key: GraphKey, values: Array[Seq[_]]) => {
            if (values(0).length > 0 ) {
              val right = values(1).head.asInstanceOf[Array[_]]
              values(0).iterator.map{ case items: Array[Any] => Tuple2(key, Array(items(1), right(2)))}
            } else {
              Seq.empty.asInstanceOf[Seq[Tuple2[GraphKey, _]]].iterator
            }
          },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(nodeType, GraphDouble.inceptorType)),
          partitionerSeq = Seq(0, 1)
        ).makeGraphRdd()
        .makeKVGraphRdd(Seq(0))
        .groupByKey(edgesRdd.partitions.length, () => new DoubleDegreeAggrBuffer())

      val u2 = vertexRdd.join(Seq(u1),
        (key: GraphKey, values: Array[Seq[_]]) => {
          if (values(1).length == 0) {
            val left = values(0).head.asInstanceOf[Array[Any]]
            Seq(Tuple2(key, Array(left(0), new DoubleWritable(0), left(2), left(3), left(4)))).iterator
          } else {
            val left = values(0).head.asInstanceOf[Array[Any]]
            val right = values(1).head.asInstanceOf[Array[Any]]

            Seq(Tuple2(key, Array(left(0), right(1), left(2), left(3), left(4)))).iterator
          }
        },
        p => p,
        partitionerSeq = Seq(0, 1)
      )
      u2.persist()

      val norm = if (normType == NormType.STANDARD) {
        val tNorm = u2.mapKVPartitionsWithContext((index, part) => {
          part.map{ case (key: GraphKey, values: Array[Any]) => {
            val t = values(1).asInstanceOf[DoubleWritable].get()
            Tuple2(key, Array(new DoubleWritable(t * t)))
          }}
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.DOUBLE))
        ).makeGraphRdd()
          .makeKVGraphRdd(Seq())
          .groupByKey(1, () => new CountDoubleAggrBuffer())
          .makeGraphRdd()
          .rdd
          .mapPartitionsWithIndex((index, part) => {
            part.map(row => row.asInstanceOf[Array[_]](0).asInstanceOf[DoubleWritable].get)
          })
          .first()

        Math.sqrt(tNorm)
      } else {
        u2.mapKVPartitionsWithIndex((index, part) => {
          part.map{ case (key: GraphKey, values: Array[Any]) => {
            Tuple2(key, Array(values(1)))
          }}
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.DOUBLE))
        ).makeGraphRdd()
          .makeKVGraphRdd(Seq())
          .groupByKey(1, () => new MaxDoubleAggrBuffer())
          .makeGraphRdd()
          .rdd
          .mapPartitionsWithIndex((index, part) => {
            part.map(row => row.asInstanceOf[Array[_]](0).asInstanceOf[DoubleWritable].get)
          })
          .first()
      }

      val u3 = u2.mapKVPartitionsWithIndex((index, part) => {
        part.map{ case (key: GraphKey, values: Array[Any]) => {
          val t = values(1).asInstanceOf[DoubleWritable].get()
          (key, Array(values(0), new DoubleWritable(t / norm), new DoubleWritable(0), values(3), values(4)))
        }}
      },
        p => p
      )
      u3.persist()

      val u4 = edgesRdd.makeKVGraphRdd(Seq(1))
        .join(Seq(u3),
          (key: GraphKey, values: Array[Seq[_]]) => {
            if (values(0).length > 0 ) {
              val right = values(1).head.asInstanceOf[Array[_]]
              values(0).iterator.map{ case items: Array[Any] => Tuple2(key, Array(items(0), right(1)))}
            } else {
              Seq.empty.asInstanceOf[Seq[Tuple2[GraphKey, _]]].iterator
            }
          },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(nodeType, GraphDouble.inceptorType)),
          partitionerSeq = Seq(1)
        ).makeGraphRdd()
        .makeKVGraphRdd(Seq(0))
        .groupByKey(edgesRdd.getReducerSize(edgesRdd.partitions.length), () => new DoubleDegreeAggrBuffer())

      val u5 = u3.join(Seq(u4),
        (key: GraphKey, values: Array[Seq[_]]) => {
          if (values(1).length == 0) {
            Seq(Tuple2(key, values(0).head)).iterator
          } else {
            val left = values(0).head.asInstanceOf[Array[Any]]
            val right = values(1).head.asInstanceOf[Array[Any]]

            Seq(Tuple2(key, Array(left(0), left(1), right(1), left(3), left(4)))).iterator
          }
        },
        p => p,
        partitionerSeq = Seq(0, 1)
      )
      u5.persist()

      val norm2 = if (normType == NormType.STANDARD) {
        val tNorm2 = u5.mapKVPartitionsWithContext((index, part) => {
          part.map{ case (key: GraphKey, values: Array[Any]) => {
            val t = values(2).asInstanceOf[DoubleWritable].get()
            Tuple2(key, Array(new DoubleWritable(t * t)))
          }}
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.DOUBLE))
        ).makeGraphRdd()
          .makeKVGraphRdd(Seq())
          .groupByKey(1, () => new CountDoubleAggrBuffer())
          .makeGraphRdd()
          .rdd
          .mapPartitionsWithIndex((index, part) => {
            part.map(row => row.asInstanceOf[Array[_]](0).asInstanceOf[DoubleWritable].get)
          })
          .first()

        Math.sqrt(tNorm2)
      } else {
        u5.mapKVPartitionsWithIndex((index, part) => {
          part.map{ case (key: GraphKey, values: Array[Any]) => {
            Tuple2(key, Array(values(2)))
          }}
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.DOUBLE))
        ).makeGraphRdd()
          .makeKVGraphRdd(Seq())
          .groupByKey(1, () => new MaxDoubleAggrBuffer())
          .makeGraphRdd()
          .rdd
          .mapPartitionsWithIndex((index, part) => {
            part.map(row => row.asInstanceOf[Array[_]](0).asInstanceOf[DoubleWritable].get)
          })
          .first()
      }

      vertexRdd = u5.mapKVPartitionsWithIndex((index, part) => {
        part.map{ case (key: GraphKey, values: Array[Any]) => {
          val t = values(2).asInstanceOf[DoubleWritable].get()
          (key, Array(values(0), values(1), new DoubleWritable(t / norm2), values(3), values(4)))
        }}
      },
        p => p
      )

      val deltaAuth = vertexRdd.mapKVPartitionsWithIndex((index, part) => {
        part.map{ case (key: GraphKey, values: Array[Any]) => {
          val a = values(1).asInstanceOf[DoubleWritable].get()
          val b = values(3).asInstanceOf[DoubleWritable].get()

          (key, Array(new BooleanWritable(Math.abs(a - b) < 0.0001)))
        }}
      },
        p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.BOOLEAN))
      ).filter{ case (key: GraphKey, values: Array[Any]) => !values(0).asInstanceOf[BooleanWritable].get()}
        .makeGraphRdd()
        .mapPartitionsWithIndex((index, part) => {
          Seq(Array(new LongWritable(part.size))).iterator
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.LONG))
        )
        .rdd
        .mapPartitionsWithIndex((index, part) => {
          part.map(row => row.asInstanceOf[Array[_]](0).asInstanceOf[LongWritable].get)
        })
        .first()

      val deltaHub = vertexRdd.mapKVPartitionsWithIndex((index, part) => {
        part.map{ case (key: GraphKey, values: Array[Any]) => {
          val a = values(2).asInstanceOf[DoubleWritable].get()
          val b = values(4).asInstanceOf[DoubleWritable].get()

          (key, Array(new BooleanWritable(Math.abs(a - b) < 0.0001)))
        }}
      },
        p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.BOOLEAN))
      ).filter{ case (key: GraphKey, values: Array[Any]) => !values(0).asInstanceOf[BooleanWritable].get()}
        .makeGraphRdd()
        .mapPartitionsWithIndex((index, part) => {
          Seq(Array(new LongWritable(part.size))).iterator
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(PrimitiveCategory.LONG))
        )
        .rdd
        .mapPartitionsWithIndex((index, part) => {
          part.map(row => row.asInstanceOf[Array[_]](0).asInstanceOf[LongWritable].get)
        })
        .first()

      vertexRdd = vertexRdd.mapKVPartitionsWithIndex((index, part) => {
        part.map{ case (key: GraphKey, values: Array[Any]) => {
          (key, Array(values(0), values(1), values(2), values(1), values(2)))
        }}
      },
        p => p
      )

      vertexRdd.persist()
      delta = deltaAuth > 0 || deltaHub > 0
      k += 1
    }

    if (authority) {
      vertexRdd.makeGraphRdd()
        .mapPartitionsWithIndex((index, part) => {
          part.map{ case values: Array[Any] => values.init}
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(nodeType, GraphDouble.inceptorType))
        )
    } else {
      vertexRdd.makeGraphRdd()
        .mapPartitionsWithIndex((index, part) => {
          part.map{ case values: Array[Any] => Array(values(0), values(2))}
        },
          p => GraphOperatorUtilities.createStandardStructOIFromInceptorType(Seq(nodeType, GraphDouble.inceptorType))
        )
    }
  }
}
