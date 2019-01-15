package com.datahack.bootcamp.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

// Descárgate el dataset para este ejemplo de este enlace: https://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption
object MungingExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Munging example")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  // Vamos a utilizar esta case class para almacenar los datos del dataset.
  // Échale un vistazo al contenido del dataset antes de empezar con el ejercicio
  case class HouseholdEPC(
                           date: String,
                           time: String,
                           gap: Double,
                           grp: Double,
                           voltage: Double,
                           gi: Double,
                           sm_1: Double,
                           sm_2: Double,
                           sm_3: Double
                         )

  // Primero vamos ha leer el contenido del fichero y crear un RDD con el.
  // Cuenta las líenas leidas del fichero.
  val hhEPCRdd: RDD[String] = ???
  val importedLines: Long = ???
  println(s"Imported lines from original dataset: $importedLines")

  // Vamos a empezar con la limpieza de los datos.
  // Primero vamos ha eliminar la cabecera del csv y despues filtramos aquellas que les falte algún dato
  // Cuando falta algún dato, en su lugar aparece el caracter "?"
  val header: String = ???
  val data: RDD[String] = ???

  // Paresea cada línea y crea un DataFrame con su contenido
  val hhEPCClassRdd: RDD[HouseholdEPC] = ???
  val hhEPCDF: DataFrame = ???

  // Muestra las 5 primeras filas para ver como ha quedado su contenido e imprime el número de filas que contiene el
  // DataFrame
  val processedLines: Long = ???
  println(s"Lines after cleaning process: $importedLines")
  // El método describe obtiene los datos estadísticos que describen el contenido del DataFrame
  hhEPCDF.describe().show()

  // Vamos a mostrar las estadísticas de los datos de todas las columnas
  // Para ello vamos a redondear a cuatro decimales todas las columnas numéricas y
  // le vamos a cambiar el nombre añadiéndole una r como prefijo (utiliza el método name)
  // No te olvides de poner la columna "summary" en la proyección de la query para saber a que corresponedn los datos
  val slectedDF: DataFrame = ???
  slectedDF.show()

  // Vamos a contar cuantos días distintos hay en el DataFrame
  val numDates: Long = ???
  println(s"Total dates: $numDates")

  // Vamos a añadir nuevas columnas a cada fila del DataFrame
  // Para ello vamos a extraer las siguientes columnas del la columna date:
  // Extrae la fecha en formato UNIX (epoc) y déjalo en la columna "dow"
  // Extrae el día y déjalo en la columna "day"
  // Extrae el mes y déjalo en la columna "month"
  // Extrae el año y déjalo en la columna "year"
  // Ayuda: utiliza el método withColumn y las funciones del objeto org.apache.spark.sql.functions
  val hhEPCDatesDf: DataFrame = ???
  hhEPCDatesDf.show(5)

  // Sobre el dataset obtenido, realiza la siguiente consulta:
  // número de registros por año y mes ordenado por año y mes ascendentemente
  val readingsByMonthDf: Dataset[Row] = ???
  readingsByMonthDf.count()
  readingsByMonthDf.show(5)

  // Vamos a realizar más operaciones, en este caso vamos a partir del DataFrame hhEPCDF
  // Primero elimina la columna "time"
  val delTmDF: DataFrame = ???

  // Realiza la siguiente consulta:
  // por cada fecha "date"
  // suma los valores de la columna "gap"
  // obten la suma de la columna "grp"
  // obten la media de la columna "voltage"
  // suma los valores de la columna "gi"
  // suma los valores de la columna "sm_1"
  // suma los valores de la columna "sm_2"
  // suma los valores de la columna "sm_3"
  // renombra las columnas añadiendo la letra d como prefijo
  // añade las columnas day, month y year a partir de la columna "date"
  val finalDayDf1: DataFrame = ???
  finalDayDf1.show(5)



}
