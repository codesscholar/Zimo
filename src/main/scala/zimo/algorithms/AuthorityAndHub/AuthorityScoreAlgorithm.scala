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
      //TODO log.info
      val afterAuthCalculateVertexRDD = calculateAuthorityScore(edgeRDD, vertexRDD, normType)
      val afterHubCalculateVertexRDD = calculateHubScore(edgeRDD, afterAuthCalculateVertexRDD, normType)

      val judgeOfAuth: Boolean =

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
      .map(e => e._2)
      .reduceByKey{
        case (a, b) => a + b
      }

    val norm = if(normType) {
      joinRDD
        .map(e => Math.sqrt(e._2))
        .reduce(_ + _)
    } else {
      joinRDD
        .map(e => e._2)
        .reduce{
          case (a, b) => if (a > b) a else b
        }
    }

    vertexRDD
      .map(e => (e.srcId, e.value))
      .leftOuterJoin(joinRDD)
      .map {
        case (key, (left, right)) => {
          val updateAuth = right.getOrElse(0.0) / norm
          new VertexType(key, (updateAuth, left._2, left._3, left._4))
        }
      }
  }


  /**
    * calculate the Hub Score, the update rule is newHub = SUM(lastAuth), which means node A.hub is  updated as the sum
    * of auth of all its neighbour
    * @param edgeRDD
    * @param vertexRDD
    * @param normType
    * @return
    */
  private def calculateHubScore(edgeRDD: RDD[EdgeType],
                                      vertexRDD: RDD[VertexType],
                                      normType: Boolean): RDD[VertexType] = {
    val edgePairRDD = edgeRDD.map(e => (e.dst, e.src))
    val vertexPairRDD = vertexRDD.map(e => (e.srcId, e.value._1))

    val joinRDD = edgePairRDD
      .join(vertexPairRDD)
      .map(e => e._2)
      .reduceByKey{
        case (a, b) => a + b
      }

    val norm = if(normType) {
      joinRDD
        .map(e => Math.sqrt(e._2))
        .reduce(_ + _)
    } else {
      joinRDD
        .map(e => e._2)
        .reduce{
          case (a, b) => if (a > b) a else b
        }
    }

    vertexRDD
      .map(e => (e.srcId, e.value))
      .leftOuterJoin(joinRDD)
      .map {
        case (key, (left, right)) => {
          val updateHub = right.getOrElse(0.0) / norm
          new VertexType(key, (left._1, updateHub, left._3, left._4))
        }
      }
  }
}
