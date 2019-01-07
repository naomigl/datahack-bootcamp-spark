package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.{SparkConf, SparkContext}

object Reduce {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)
    thirdExample(sc)

    sc.stop()
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  // Obtener la suma de los elementos de un RDD de enteros utilizando el método reduce
  def firstExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 100, 3)

    val spartitions = a.mapPartitionsWithIndex(myfunc).collect()
    val result = a.reduce(_ + _)

    println("---------- Example 1 ----------")
    spartitions.foreach(println)
    println(s"--Result: $result")
  }

  // Obtener la suma de los elementos de un RDD de enteros utilizando el método fold
  def secondExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 100, 3)

    val spartitions = a.mapPartitionsWithIndex(myfunc).collect()
    val result = a.fold(0)(_ + _)

    println("---------- Example 2 ----------")
    spartitions.foreach(println)
    println(s"--Result: $result")
  }

  // Obtener la suma de los elementos de un RDD de enteros utilizando el método reduce con un valor zero distinto
  // del valor identidad de la suma.
  def thirdExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 100, 3)

    val spartitions = a.mapPartitionsWithIndex(myfunc).collect()
    val result = a.fold(10)(_ + _)

    println("---------- Example 3 ----------")
    spartitions.foreach(println)
    println(s"--Result: $result")
  }

}
