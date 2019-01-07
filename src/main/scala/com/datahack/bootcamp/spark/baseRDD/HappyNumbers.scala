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
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    // Le el contenido del fichero quijote.txt que encontraras en la carpeta resouces en un RDD.
    val file: RDD[String] = ???
    // Obten un RDD con todas las palabras del contenidas en las frases del RDD file
    val words: RDD[String] = ???

    // Vamos a realizar el ejercicio de diferentes formas
    firstExercise(words)
    secondExercise(words)
    thirdExercise(words)
  }

  // Realiza el ejercicio utilizando las funciones .map() y .filter()
  def firstExercise(words: RDD[String]): Unit = ???

  // Realiza el ejercicio utilizando las funciones .map() y .aggregate()
  def secondExercise(words: RDD[String]): Unit = ???

  // Para este último ejercicio indica si el resultado de la suma de todos los números felices encontrados
  // Es a su vez un número feliz
  def thirdExercise(words: RDD[String]): Unit = ???
}

// Vamos a utilizar esta clase como clase de utilería para realizar calcular los números felices
object Happy {

  // TODO: dado un caracter devuelve su posición en el abecedario
  // Consejo: si el caracter no se corresponde con una letra o es una Ñ, devuelve un 0.
  def charToInt(c: Char): Int = ???

  // TODO: este médoto dado un entero nos indica si se trata de un número feliz o no.
  def happy(n: Int, visited: List[Int] = Nil): Boolean = ???

}
