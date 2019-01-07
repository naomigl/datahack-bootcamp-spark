package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.{SparkConf, SparkContext}

object Reduce {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

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
  def firstExample(sc: SparkContext): Unit = ???

  // Obtener la suma de los elementos de un RDD de enteros utilizando el método fold
  def secondExample(sc: SparkContext): Unit = ???

  // Obtener la suma de los elementos de un RDD de enteros utilizando el método reduce con un valor zero distinto
  // del valor identidad de la suma.
  def thirdExample(sc: SparkContext): Unit = ???

}
