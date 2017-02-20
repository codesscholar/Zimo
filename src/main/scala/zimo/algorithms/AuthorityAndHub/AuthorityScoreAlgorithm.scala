package zimo.algorithms.AuthorityAndHub

import org.apache.spark.rdd.RDD

/**
  * Created by wangzhiping on 17-2-20.
  */
object AuthorityScoreAlgorithm {

  def runAuthorityHubScore(edgeRDD: RDD[EdgeType],
                           vertexRDD: RDD[VertexType],
                           iteration: Int,
                           normType: Boolean): RDD[VertexType] = {

    require(iteration > 0, "The iteration num must > 0")
    var k = 0
    var delta = true
    while(k < iteration && delta) {


    }


    null
  }

  /**
    * calculate the Authority Score, the update rule is newAuth = SUM(lastHub), which means node A.auth update as the sum
    * of hub of all its neighbour which
    * @param edgeRDD
    * @param vertexRDD
    * @param normType
    * @return
    */
  private def calculateAuthorityScore(edgeRDD: RDD[EdgeType],
                                      vertexRDD: RDD[VertexType],
                                      normType: Boolean): RDD[VertexType] = {
    val edgePairRDD = edgeRDD.map(e => (e.src, e.dst))
    val vertexPairRDD = vertexRDD.map(e => (e.srcId, e.value._2))

    val joinRDD = edgePairRDD
      .join(vertexPairRDD)
      .reduceByKey{
        case (a, b) => (a._1, a._2 + b._2)
      }

    val norm = if(normType) {
      joinRDD
        .map(e => Math.sqrt(e._2._2))
        .reduce(_ + _)
    } else {
      joinRDD
        .map(e => e._2._2)
        .reduce{
          case (a, b) => if (a > b) a else b
        }
    }

    vertexRDD
      .map(e => (e.srcId, e.value))
      .leftOuterJoin(joinRDD)
      .map {
        case (key, (left, right)) => {
          val updateAuth = right.getOrElse(Tuple2(key, 0.0))._2 / norm
          new VertexType(key, (updateAuth, left._2, left._3, left._4))
        }
      }
  }
}
