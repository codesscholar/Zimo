package zimo.algorithms

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

import SerializableHadleObject.{serializeMap => SM, derializeMap => DM }

/**
  * Created by wzp on 17-02-20.
  */
class BalancedMultiLabelPropagationAlgorithm {

  import BalancedMultiLabelPropagationAlgorithm._

  def runBmlpa(filePath: String, sc: SparkContext, numOfPartition: Int): Unit = {
    val graph = initialGraph(filePath, sc, numOfPartition)
    val edgeRdd = graph._1
    val vertextRdd = graph._2

    println("edge num " +  edgeRdd.count())
    println("vertext num " + vertextRdd.count())

    val edgeWithoutWeightRdd = edgeRdd
      .map(e => (e._1, e._2))
      .partitionBy(new org.apache.spark.HashPartitioner(numOfPartition))
    edgeWithoutWeightRdd.persist()

    val roughcore = roughCore(edgeWithoutWeightRdd)
    println("roughcore " + roughcore.count())

    var oldVertextRdd = initialVertexRdd(vertextRdd, roughcore, numOfPartition)
    println("update vertextRdd done")

    var lastMap = count(oldVertextRdd)

    var lastMin = lastMap

    var numOfIteration: Int = 1
    var flag = true
    while(flag) {
      println(" start the ith iteration " + numOfIteration)
      val newVertextRdd = updateCommunity(oldVertextRdd, edgeWithoutWeightRdd)

      newVertextRdd.persist()
      // checkpoint newVertextRdd to shorter the chain
      if (numOfIteration % 15 == 0) {
        //newVertextRdd.count()
        newVertextRdd.checkpoint()
      }

      val currentMap = count(newVertextRdd)

      if (currentMap.count() == lastMap.count()) {
        val newMin = mc(currentMap, lastMap)
        if (checkEquals(lastMin, newMin)) {
          flag = false
        } else {
          lastMin = newMin
        }
      } else {
        lastMin = currentMap
      }
      lastMap = currentMap

      oldVertextRdd = newVertextRdd
      numOfIteration += 1
    }
    calculateFinalResult(oldVertextRdd)
    println("num of iterations of label progation : " + numOfIteration)

    // TODO handle com and sub
  }

  /**
    * 算法终止条件:对于每个出现的标签,属于该标签的点的数量也全部一样则停止(注：本num是经过MC过后的数量)
    * @param oldMinRdd (commId, numOfNode)
    * @param newMinRdd (commId, numOfNode)
    * @return　true if 符合终止条件
    */
  def checkEquals(oldMinRdd: RDD[(Int, Int)], newMinRdd: RDD[(Int, Int)]): Boolean = {

    // 采用fullOuterJoin的形式,避免(A, B, C)和(B, C, D)的错误判断
    oldMinRdd.fullOuterJoin(newMinRdd).map{
      case(labelId, (oldNum, newNum)) => {
        if (oldNum.getOrElse(0) == newNum.getOrElse(0)) {
          0
        } else {
          1
        }
      }
    }.reduce(_ + _) == 0
  }

  def calculateFinalResult(vertextRdd: RDD[(Int, Array[Byte])]) = {
    // way 1 : every node chose the best community which has the biggest possibility
    val afterChosenRdd = vertextRdd
      .map{
        case (key, value) => {
          val map = DM(value)
          var maxId = -1
          var maxPossibility = -1.0
          map.foreach{
            case (id, possibility) => {
              if (possibility > maxPossibility) {
                maxId = id
                maxPossibility = possibility
              }
            }
          }
          (maxId, 1)}
      } // map end
      .groupByKey()
      .map(e => (e._1, e._2.size)) // (idOfCommunity, numOfCommunity)

    val afterChosenAndFilterRdd = afterChosenRdd.filter(e => e._2 >= 3)

    println("Community size after chosen is " + afterChosenRdd.count())
    println("Community size after chosen and  >= 3 is " + afterChosenAndFilterRdd.count())


    val communitySize = vertextRdd
      .map(e => DM(e._2))
      .reduce((a, b) => unionMap(a, b))
      .keySet.size
    println("Community size " + communitySize)
  }

  /**
    * one iteration of update community of each vertext
    * @param oldVertextRdd
    * @param edgeRdd
    * @return newVertextRdd
    */
  def updateCommunity(oldVertextRdd: RDD[(Int, Array[Byte])], edgeRdd: RDD[(Int, Int)]): RDD[(Int, Array[Byte])] = {

    edgeRdd
      .join(oldVertextRdd) // (src, (dst, labelofsrc))
      .map(e => (e._2._1, e._2._2)) //(dst, labelOfSrc)
      .reduceByKey((a, b) => SM(unionMap(DM(a),DM(b))))
      .mapValues(e => SM(normalizeMap(DM(e))))
    }

  /**
    * 从一个点RDD统计出所有的标签（社区号）,并统计出每个标签包含的点的数量
    * @param vertextRdd
    * @return　map(labelId, numOfNodes)
    */
  def count(vertextRdd: RDD[(Int, Array[Byte])]): RDD[(Int, Int)] = {

    vertextRdd
      .flatMap{
        case (id, labels) => {
         DM(labels).map(e => (e._1, 1))
        }
      }
      .reduceByKey(_ + _)
  }

  // 用于终止条件的一部分
  def mc(oldMapRdd: RDD[(Int, Int)], newMapRdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {

    oldMapRdd
      .join(newMapRdd)
      .mapValues{ case (oleNum, newNum) => Math.min(oleNum, newNum) }
  }

  /**
    * 初始化标签，对于RC操作后的团内的节点共用一个社区号，其余的用自己的ID作为社区号
    * @param vertextRdd (nodeId, commId)
    * @param roughCore (commId, Array)
    * @return
    */
  def initialVertexRdd(vertextRdd: RDD[(Int, Int)], roughCore: RDD[(Int, ArrayBuffer[Int])], numOfPartition: Int):
    RDD[(Int, Array[Byte])] = {

    // roughCore需要rePartition一次
    val vertextToCommunityRdd = roughCore
      .repartition(numOfPartition)
      .flatMap{
        case (key, array) => {
          val v2c = ArrayBuffer[(Int, Int)]()
          array.foreach(e => v2c.append((e, key)))
          v2c
        }
      }
      .groupByKey()

    vertextRdd
      .leftOuterJoin(vertextToCommunityRdd)
      .mapValues{
        case (noCoreId, coreArray) => {
          val tempArray = coreArray.getOrElse(Array(noCoreId).toIterable)
          val num = tempArray.size
          var coreMap = Map.empty[Int, Double]
          tempArray.foreach(e => coreMap += (e -> 1.0 / num))
          SM(coreMap)
        }
      }
      .partitionBy(new org.apache.spark.HashPartitioner(numOfPartition))
//      .groupByKey
//      .mapValues(iter => iter.reduce((A1, A2) => A1))
  }

  /**
    * RC操作
    * @param edgeRdd
    * @return 团的集合
    */
  def roughCore(edgeRdd: RDD[(Int, Int)]): RDD[(Int, ArrayBuffer[Int])] = {

    // first step: rePartition to one partirion
    val onePartitionEdgeRdd = edgeRdd.groupBy( _._1).repartition(1)

    // initial vertextDegree and neighbours
    onePartitionEdgeRdd.mapPartitions(iters => {
      var vertextDegree = ArrayBuffer[(Int, Int)]()
      val neighbours = new mutable.HashMap[Int, (Boolean, ArrayBuffer[Int])]()

      iters.foreach{
        case(vertextId, iterOfNeighbour) => {
          val neighbour = new ArrayBuffer[Int]()
          iterOfNeighbour.foreach(e => neighbour.append(e._2))

          neighbours(vertextId) = (false, neighbour)
          vertextDegree.append((vertextId, neighbour.length))

        }
      }

      // sort vertextDegree by degree from big to small
      vertextDegree = vertextDegree.sortWith((e1, e2) => e1._2 > e2._2)

      val cores = ArrayBuffer[(Int, ArrayBuffer[Int])]()

      // check
      def checkVertextId(vertextId: Int): Boolean = {
        !neighbours(vertextId)._1 && neighbours(vertextId)._2.length >= THREAD_K
      }
      // traversing
      for(item <- vertextDegree) {
        val core = ArrayBuffer[Int]()
        val vertextId = item._1

        if (checkVertextId(vertextId)) {
          // determine A, then find B (has the max degree in neighbour(A))
          val neighbour = neighbours(vertextId)._2
          var maxNeighbourId: Int = -1
          var maxNeighbourDegree: Int = -1
          neighbour.foreach(id => {
            if (checkVertextId(id)) {
              val tempDegree = neighbours(id)._2.length
              if (tempDegree > maxNeighbourDegree) {
                maxNeighbourId = id
                maxNeighbourDegree = tempDegree
              }
            }
          })

          if ( maxNeighbourId != -1) {
            // println("neighbourID " + maxNeighbourId)
            val neighbourOfA = neighbours(vertextId)._2
            val neighbourOfB = neighbours(maxNeighbourId)._2

            neighbours(vertextId) = (true, neighbourOfA)
            neighbours(maxNeighbourId) = (true, neighbourOfA)

            core.append(vertextId)
            core.append(maxNeighbourId)

            // traversing all the neighbour of A(vertextId) and B(maxNeighbourId)
            var candiNeighbour = Map.empty[Int, Int]
            neighbourOfA.foreach(e => {
              candiNeighbour += (e -> 1)
            })
            neighbourOfB.foreach(e => {
              if (!candiNeighbour.contains(e)) {
                candiNeighbour += (e -> 1)
              } else {
                candiNeighbour(e) = 2
              }
            })

            // println("candi ")
            // candiNeighbour.foreach(println(_.toString))
            while(candiNeighbour.size != 0) {
              // 随机取一个元素(Id, num)
              val temp = candiNeighbour.head
              val tempId = temp._1
              val tempNum = temp._2

              if (tempNum == 2) { //筛选出共同的邻居节点
                val tempNeighbour = neighbours(tempId)._2
                core.append(tempId)
                neighbours(tempId) = (true, tempNeighbour)

                // delete vertices not in N(tempId) from candiNeighbour
                candiNeighbour.foreach(e => {
                  if (!tempNeighbour.contains(e._1)) {
                    candiNeighbour.remove(e._1)
                  }
                })
              }
              candiNeighbour.remove(tempId)
            }

          }
          if (core.size >= 2)
            cores.append((maxNeighbourId, core))
        }
      }
      cores.iterator
    })
  }

  /**
    * 初始化边RDD
    * @param filePath 文件路径
    * @param sc　
    * @return　edgeRdd(src, dst, weight)
    */
  def initialGraph(filePath: String, sc: SparkContext, numOfPartition: Int): (RDD[(Int, Int, Int)], RDD[(Int, Int)]) = {

    val source = sc.textFile(filePath, numOfPartition)
    val edgeRdd = source
      .flatMap(row => {
        val columns = row.split("\t")
        if (columns.length < 2)
          throw(new IllegalArgumentException("Data line error" + row))
        Array(
          (columns(0).toInt, columns(1).toInt, 1),
          (columns(1).toInt, columns(0).toInt, 1)
        )}
      )
      .distinct()

    val vertextRdd = edgeRdd
      .flatMap(e => Array((e._1, e._1), (e._2, e._2)))
      .distinct()
    (edgeRdd, vertextRdd)
  }
}


object BalancedMultiLabelPropagationAlgorithm {
  val THREAD_K: Int = 3
  val THREAD_P: Double = 0.75

  def normalizeMap(a: Map[Int, Double]): Map[Int, Double] = {
    val maxPossibility: Double = a.values.reduce((a, b) => if(a > b) a else b)

    val resultMap = Map.empty[Int, Double]
    var sumPossibility = 0.0
    a.foreach{ case(key: Int, value: Double) => {
      val newValue = value / maxPossibility
      if (newValue >= THREAD_P) {
        resultMap += (key -> newValue)
        sumPossibility += newValue
      }
    }}
    resultMap.keys.foreach(resultMap(_) /= sumPossibility)
    resultMap
  }

  /**
    * 标签合并时用来合并两个邻居的标签
    * @param a 邻居a的标签
    * @param b 邻居b的标签
    * @return 合并后的标签
    */
  def unionMap(a: Map[Int, Double], b: Map[Int, Double]): Map[Int, Double] = {

    b.foreach{ case(key, value) => {
      if (a.contains(key)) {
        a(key) += value
      } else {
        a += (key -> value)
      }
    }}

    a
  }
}