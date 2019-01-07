package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Queremos saber que palabras distintas del un fichero dado tienen una representación numérica de un número feliz.
// La representación numérica de una palara es el resutado de la suma de la posición de cada una de sus letras el el abcedario.
// Un número feliz es: https://matesmates.wordpress.com/2015/02/21/numeros-felices/
// El resultado es una lista de valores ("palabra en mayúsculas", happy number)
// Utiliza las funciones .map(), .flatMap(), .filter()

//https://www.gutenberg.org/cache/epub/2000/pg2000.txt


object HappyDslImpl extends App {

  val conf = new SparkConf()
    .setAppName("Happy Numbers DSL")
    .setMaster("local[2]")
  implicit val sc: SparkContext = new SparkContext(conf)

  val file: RDD[String] = sc.textFile("src/main/resources/quijote.txt")
  val words: RDD[String] = file.flatMap(_.split(" ")).map(_.toUpperCase).distinct().cache()

}

