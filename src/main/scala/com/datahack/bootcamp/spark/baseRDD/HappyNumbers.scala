package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HappyNumbers {

  // Queremos saber que palabras distintas del un fichero dado tienen una representación numérica de un número feliz.
  // La representación numérica de una palara es el resutado de la suma de la posición de cada una de sus letras el el abcedario.
  // En este enlace puedes encontrar como se calcula un número feliz: https://matesmates.wordpress.com/2015/02/21/numeros-felices/
  // El resultado es una lista de valores ("palabra en mayúsculas", happy number)

  // Si quieres ejecutarlo con el contenido completo del quijote, en este enlace puedes
  // descargártelo entero en formato txt: https://www.gutenberg.org/cache/epub/2000/pg2000.txt

  def main(args: Array[String]) {

    // Crea un contexto de Spark a partir de su configuración
    val conf = new SparkConf()
      .setAppName("Happy Numbers")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Le el contenido del fichero quijote.txt que encontraras en la carpeta resouces en un RDD.
    val file: RDD[String] = sc.textFile("src/main/resources/quijote.txt")
    // Obten un RDD con todas las palabras del contenidas en las frases del RDD file
    val words: RDD[String] = file.flatMap(_.split(" ")).map(_.toUpperCase).distinct().cache()

    // Vamos a realizar el ejercicio de diferentes formas
    firstExercise(words)
    secondExercise(words)
    thirdExercise(words)
  }

  // Realiza el ejercicio utilizando las funciones .map() y .filter()
  def firstExercise(words: RDD[String]) = {
    val happyWords: Array[(String, Int)] = words.map(w => (w, w.toList.map(Happy.charToInt).sum))
      .map(w => (w._1, w._2, Happy.happy(w._2, List.empty)))
      .filter(_._3).map(w => (w._1, w._2)).collect()

    println("------Result: ")
    happyWords.foreach(println)
  }

  // Realiza el ejercicio utilizando las funciones .map() y .aggregate()
  def secondExercise(words: RDD[String]) = {
    val happyWords: Array[(String, Int)] = words.map(w => (w, w.toList.map(Happy.charToInt).sum))
      .map(w => (w._1, w._2, Happy.happy(w._2, List.empty)))
      .filter(_._3).map(w => (w._1, w._2)).collect()

    println("------Result: ")
    happyWords.foreach(println)
  }

  // Para este último ejercicio indica si el resultado de la suma de todos los números felices encontrados
  // Es a su vez un número feliz
  def thirdExercise(words: RDD[String]) = {
    val happyWords = words.map(w => (w, w.toList.map(Happy.charToInt).sum))
      .map(w => (w._1, w._2, Happy.happy(w._2, List.empty)))
      .filter(_._3).map(_._2).reduce(_ + _)

    println(s"------Result: $happyWords is happy number: ${Happy.happy(happyWords, List.empty)}")
  }
}

// Vamos a utilizar esta clase como clase de utilería para realizar calcular los números felices
object Happy {

  // TODO: dado un caracter devuelve su posición en el abecedario
  // Consejo: si el caracter no se corresponde con una letra o es una Ñ, devuelve un 0.
  def charToInt(c: Char): Int = c match {
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

  def sumSquare(ds: List[Int]): Int = ds map (d => d * d) sum

  def digits(n: Int): List[Int] = if (n < 10) List(n) else (n % 10) :: digits(n / 10)

  // TODO: este médoto dado un entero nos indica si se trata de un número feliz o no.
  def happy(n: Int, visited: List[Int] = Nil): Boolean = sumSquare(digits(n)) match {
    case 1 => true
    case n => if (visited contains n) false else happy(n, n :: visited)
  }

  /*def main(args: Array[String]) {
    val happys = Set(1, 7, 10, 13, 19, 23, 28, 31, 32, 44, 49, 68, 70, 79, 82, 86, 91, 94, 97, 100, 103, 109, 129, 130, 133, 139, 167, 176, 188, 190, 192, 193, 203, 208, 219, 226, 230, 236, 239, 262, 263, 280, 291, 293, 301, 302, 310, 313, 319, 320, 326, 329, 331, 338, 356, 362, 365, 367, 368, 376, 379, 383, 386, 391, 392, 397, 404, 409, 440, 446, 464, 469, 478, 487, 490, 496)

    (1 to 500).toList map (n => (n, happy(n))) foreach { case (i, p) => {
      val passes = p == (happys contains i)
      if(!passes)
        println("happy fails for " + i)
    }}
  }*/
}
