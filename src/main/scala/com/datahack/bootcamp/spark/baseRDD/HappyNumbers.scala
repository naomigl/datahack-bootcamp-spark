package com.datahack.bootcamp.spark.baseRDD

import com.datahack.bootcamp.spark.pairRDD.SimpleJoin.channelFilesPathUrl
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HappyNumbers {

  // Queremos saber que palabras distintas del un fichero dado tienen una representación numérica de un número feliz.
  // La representación numérica de una palara es el resutado de la suma de la posición de cada una de sus letras.
  // Un número feliz es un número entero positivo
  // El resultado es una lista de valores ("palabra en mayúsculas", happy number)
  // Utiliza las funciones .map(), .flatMap(), .filter()

  //https://www.gutenberg.org/cache/epub/2000/pg2000.txt

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Happy Numbers")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("src/main/resources/quijote.txt")
    val l: RDD[Array[String]] = file.map(_.split(" "))
    val words: RDD[String] = file.flatMap(_.split(" ")).map(_.toUpperCase).distinct()

    val happyWords: Array[(String, Int)] = words.map(w => (w, w.toList.map(Happy.charToInt).sum))
      .map(w => (w._1, w._2, Happy.happy(w._2, List.empty)))
      .filter(_._3).map(w => (w._1, w._2)).collect()

    println("------Result: ")
    happyWords.foreach(println)

  }
}

object Happy {

  def charToInt(c: Char): Int = 22

  /*
    A=1
    B=2
    C=3
    D=4
    E=5
    F=6
    G=7
    H=8
    I =9
    J = 10
    K = 11
    L = 12
    M = 13
    N = 14
    O = 15
    P = 16
    Q = 17
    R = 18
    S = 19
    T = 20
    U = 21
    V = 22
    W = 23
    X = 24
    Y = 25
    Z = 26
   */

  def sumSquare(ds: List[Int]): Int = ds map (d => d * d) sum

  def digits(n: Int): List[Int] = if (n < 10) List(n) else (n % 10) :: digits(n / 10)

  def happy(n: Int, visited: List[Int] = Nil): Boolean = sumSquare(digits(n)) match {
    case 1 => true
    case n => if (visited contains n) false else happy(n, n :: visited)
  }

  def main(args: Array[String]) {
    val happys = Set(1, 7, 10, 13, 19, 23, 28, 31, 32, 44, 49, 68, 70, 79, 82, 86, 91, 94, 97, 100, 103, 109, 129, 130, 133, 139, 167, 176, 188, 190, 192, 193, 203, 208, 219, 226, 230, 236, 239, 262, 263, 280, 291, 293, 301, 302, 310, 313, 319, 320, 326, 329, 331, 338, 356, 362, 365, 367, 368, 376, 379, 383, 386, 391, 392, 397, 404, 409, 440, 446, 464, 469, 478, 487, 490, 496)

    (1 to 500).toList map (n => (n, happy(n))) foreach { case (i, p) => {
      val passes = p == (happys contains i)
      if(!passes)
        println("happy fails for " + i)
    }}
  }
}
