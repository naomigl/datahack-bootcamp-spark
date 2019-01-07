package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Queremos saber que palabras distintas del un fichero dado tienen una representación numérica de un número feliz.
// La representación numérica de una palara es el resutado de la suma de la posición de cada una de sus letras el el abcedario.
// Un número feliz es: https://matesmates.wordpress.com/2015/02/21/numeros-felices/
// El resultado es una lista de valores ("palabra en mayúsculas", happy number)
// Utiliza las funciones .map(), .flatMap(), .filter()

//https://www.gutenberg.org/cache/epub/2000/pg2000.txt

case class HappyNumber(word: String, value: Int, isHappy: Boolean) {
  override def toString: String = {
    s"$word, $value"
  }
}

class WordToIntConverter(self: RDD[String]) (implicit sc: SparkContext) extends Serializable {

  private def charToInt(c: Char): Int = c match {
    case 'A' | 'Á' => 1
    case 'B' => 2
    case 'C' => 3
    case 'D' => 4
    case 'E' | 'É' => 5
    case 'F' => 6
    case 'G' => 7
    case 'H' => 8
    case 'I' | 'Í'=> 9
    case 'J' =>  10
    case 'K' => 11
    case 'L' => 12
    case 'M' => 13
    case 'N' => 14
    case 'O' | 'ó'=> 15
    case 'P' => 16
    case 'Q' => 17
    case 'R' => 18
    case 'S' => 19
    case 'T' => 20
    case 'U' | 'Ú'=> 21
    case 'V' => 22
    case 'W' => 23
    case 'X' => 24
    case 'Y' => 25
    case 'Z' => 26
    case _ => 0
  }

  def toInt: RDD[(String, Int)] = {
    self.map(w => (w, w.toList.map(charToInt).sum))
  }
}

class HappyNumbersUtils(self: RDD[(String, Int)]) (implicit sc: SparkContext) extends Serializable {

  private def sumSquare(ds: List[Int]): Int = ds map (d => d * d) sum

  private def digits(n: Int): List[Int] = if (n < 10) List(n) else (n % 10) :: digits(n / 10)

  private def happy(n: Int, visited: List[Int] = Nil): Boolean = sumSquare(digits(n)) match {
    case 1 => true
    case n => if (visited contains n) false else happy(n, n :: visited)
  }

  def filterHappyNumber: RDD[HappyNumber] = {
    self.map(w => HappyNumber(w._1, w._2, Happy.happy(w._2, List.empty))).filter(_.isHappy)
  }
}

trait HappyDsl {

  implicit def intConverter(words: RDD[String])
                           (implicit sc: SparkContext): WordToIntConverter = new WordToIntConverter(words)

  implicit def happyNumber(words: RDD[(String, Int)])
                          (implicit sc: SparkContext): HappyNumbersUtils = new HappyNumbersUtils(words)
}

object HappyDslImpl extends App with HappyDsl {

  val conf = new SparkConf()
    .setAppName("Happy Numbers DSL")
    .setMaster("local[2]")
  implicit val sc: SparkContext = new SparkContext(conf)

  val file: RDD[String] = sc.textFile("src/main/resources/quijote.txt")
  val words: RDD[String] = file.flatMap(_.split(" ")).map(_.toUpperCase).distinct().cache()

  val result = words.toInt.filterHappyNumber.collect()

  println("------Result: ")
  result.foreach(println)

}

