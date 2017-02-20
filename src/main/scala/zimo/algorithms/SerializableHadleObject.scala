package zimo.algorithms

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.Map

/**
  * Created by wangzhiping on 16-12-1.
  */
object SerializableHadleObject {
  def serializeTuple2Object(value: Tuple2[Int, Double]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = new ObjectOutputStream(byteArrayOutputStream)
    outputStream.writeInt(value._1)
    outputStream.writeDouble(value._2)
    outputStream.close()
    byteArrayOutputStream.toByteArray
  }

  def deserializeTuple2Object(bytes: Array[Byte]): Tuple2[Int, Double] = {
    val byteArrayInputStream = new ByteArrayInputStream(bytes)
    val inputStream = new ObjectInputStream(byteArrayInputStream)
    val value = (inputStream.readInt(), inputStream.readDouble())
    inputStream.close()
    value
  }

  def serializeMap(map: Map[Int, Double]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val outputStream = new ObjectOutputStream(byteArrayOutputStream)
    map.foreach{ case (key: Int, value: Double) => {
      outputStream.writeInt(key)
      outputStream.writeDouble(value)
    }}
    outputStream.writeInt(-1)
    outputStream.close()
    byteArrayOutputStream.toByteArray
  }

  def derializeMap(bytes: Array[Byte]): Map[Int, Double] = {
    val byteArrayInputStream = new ByteArrayInputStream(bytes)
    val inputStream = new ObjectInputStream(byteArrayInputStream)
    var resultMap = Map.empty[Int, Double]
    var key = inputStream.readInt()
    var value: Double = 0.0
    while (key != -1) {
      value = inputStream.readDouble()
      resultMap += (key -> value)
      key = inputStream.readInt()
    }
    inputStream.close()
    resultMap
  }


  def main(args: Array[String]): Unit = {
    val map: Map[Int, Double] = Map(1 -> 1.0, 2 -> 2.0)
    val byteArray = serializeMap(map)
    val resultMap = derializeMap(byteArray)
    println(resultMap.toString())
  }
}
