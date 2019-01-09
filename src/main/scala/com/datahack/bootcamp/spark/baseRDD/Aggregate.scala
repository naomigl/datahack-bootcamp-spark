package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("aggregate").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)
    thirdExample(sc)
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  // Este ejemplo devuelve 16 ya que su valor inicial es 5
  // la agregación de la particion 0 será max(5, 1, 2, 3) = 5
  // la agregación de la particion 1 será max(5, 4, 5, 6) = 6
  // la agregación final de todas las particiones será 5 + 5 + 6 = 16
  // Nota: la agregación final incluye el valo inicial
  def firstExample(sc: SparkContext): Unit = {
    val z = sc.parallelize(List(1, 2, 3, 4, 5,6), 2)

    val spartitions = z.mapPartitionsWithIndex(myfunc).collect()

    val result = z.aggregate(0)(math.max(_,_), _ + _)
    val result2 = z.aggregate(5)(math.max(_,_), _ + _)

    println("----Example 1 -----")
    spartitions.foreach(println)
    println(s"--Result(0): $result")
    println(s"--Result(5): $result2")
  }

  // Ahora el valor inicial "x" se aplica tres veces.
  //  - una para cada partición
  //  - una para combinar todas las particiones en la segunda función.
  def secondExample(sc: SparkContext): Unit = {
    val z = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 2)

    val spartions = z.mapPartitionsWithIndex(myfunc).collect()

    val result = z.aggregate("")(_ + _, _ + _)
    val result2 = z.aggregate("x")(_ + _, _ + _)

    println("----Example 2 ------")
    spartions.foreach(println)
    println(s"--Result: $result")
    println(s"--Result(x): $result2")
  }

  def thirdExample(sc: SparkContext): Unit = {
    val z = sc.parallelize(List("12", "23", "345", "4567"), 2)

    val spartition = z.mapPartitionsWithIndex(myfunc).collect()

    val result = z.aggregate(0)((x: Int, y: String) => math.max(x, y.length), _ + _)

    println("-----Example 3 ----")
    spartition.foreach(println)
    println(s"Result: $result")
  }

  def fourthExample(sc: SparkContext): Unit = ???

}
