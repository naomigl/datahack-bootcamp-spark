package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

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
  def firstExample(sc: SparkContext): Unit = ???

  // Ahora el valor inicial "x" se aplica tres veces.
  //  - una para cada partición
  //  - una para combinar todas las particiones en la segunda función.
  def secondExample(sc: SparkContext): Unit = ???

  def thirdExample(sc: SparkContext): Unit = ???

  def fourthExample(sc: SparkContext): Unit = ???

}
