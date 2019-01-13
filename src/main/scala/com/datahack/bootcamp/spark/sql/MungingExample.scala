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
  val hhEPCRdd: RDD[String] = sc.textFile("src/main/resources/household_power_consumption.txt")
  val importedLines: Long = hhEPCRdd.count()
  println(s"Imported lines from original dataset: $importedLines")

  // Vamos a empezar con la limpieza de los datos.
  // Primero vamos ha eliminar la cabecera del csv y despues filtramos aquellas que les falte algún dato
  // Cuando falta algún dato, en su lugar aparece el caracter "?"
  val header: String = hhEPCRdd.first()
  val data: RDD[String] = hhEPCRdd.filter(row => row != header).filter(rows => !rows.contains("?"))

  // Paresea cada línea y crea un DataFrame con su contenido
  val hhEPCClassRdd: RDD[HouseholdEPC] = data.map(_.split(";"))
    .map(p => HouseholdEPC(p(0).trim().toString,p(1).trim().toString,p(2).toDouble,p(3).toDouble,p(4).toDouble,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble))
  val hhEPCDF: DataFrame = hhEPCClassRdd.toDF()

  // Muestra las 5 primeras filas para ver como ha quedado su contenido e imprime el número de filas que contiene el
  // DataFrame
  hhEPCDF.show(5)
  val processedLines: Long = hhEPCDF.count()
  println(s"Lines after cleaning process: $importedLines")
  // El método describe obtiene los datos estadísticos que describen el contenido del DataFrame
  hhEPCDF.describe().show()

  // Vamos a mostrar las estadísticas de los datos de todas las columnas
  // Para ello vamos a redondear a cuatro decimales todas las columnas numéricas y
  // le vamos a cambiar el nombre añadiéndole una r como prefijo (utiliza el método name)
  // No te olvides de poner la columna "summary" en la proyección de la query para saber a que corresponedn los datos
  hhEPCDF.describe().select(
    $"summary",
    round($"gap", 4).name("rgap"),
    round($"grp", 4).name("rgrp"),
    round($"voltage", 4).name("rvoltage"),
    round($"gi", 4).name("rgi"),
    round($"sm_1", 4).name("rsm_1"),
    round($"sm_2", 4).name("rsm_2"),
    round($"sm_3", 4).name("rsm_3"))
    .show()

  // Vamos a contar cuantos días distintos hay en el DataFrame
  val numDates = hhEPCDF.groupBy("date").agg(countDistinct("date")).count()
  println(s"Total dates: $numDates")

  // Vamos a añadir nuevas columnas a cada fila del DataFrame
  // Para ello vamos a extraer las siguientes columnas del la columna date:
  // Extrae la fecha en formato UNIX (epoc) y déjalo en la columna "dow"
  // Extrae el día y déjalo en la columna "day"
  // Extrae el mes y déjalo en la columna "month"
  // Extrae el año y déjalo en la columna "year"
  // Ayuda: utiliza el método withColumn y las funciones del objeto org.apache.spark.sql.functions
  val hhEPCDatesDf: DataFrame = hhEPCDF
    .withColumn("dow", from_unixtime(unix_timestamp($"date", "dd/MM/yyyy"), "EEEEE"))
    .withColumn("day", dayofmonth(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
    .withColumn("month", month(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
    .withColumn("year", year(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
  hhEPCDatesDf.show(5)

  // Sobre el dataset obtenido, realiza la siguiente consulta:
  // número de registros por año y mes ordenado por año y mes ascendentemente
  val readingsByMonthDf: Dataset[Row] = hhEPCDatesDf
    .groupBy($"year", $"month").count().orderBy($"year", $"month")
  readingsByMonthDf.count()
  readingsByMonthDf.show(5)

  // Vamos a realizar más operaciones, en este caso vamos a partir del DataFrame hhEPCDF
  // Primero elimina la columna "time"
  val delTmDF = hhEPCDF.drop("time")

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
  val finalDayDf1: DataFrame = delTmDF
    .groupBy($"date")
    .agg(sum($"gap").name("A"),
      sum($"grp").name("B"),
      avg($"voltage").name("C"),
      sum($"gi").name("D"),
      sum($"sm_1").name("E"),
      sum($"sm_2").name("F"),
      sum($"sm_3").name("G"))
    .select($"date",
      round($"A", 2).name("dgap"),
      round($"B", 2).name("dgrp"),
      round($"C", 2).name("dvoltage"),
      round($"C", 2).name("dgi"),
      round($"E", 2).name("dsm_1"),
      round($"F", 2).name("dsm_2"),
      round($"G", 2).name("dsm_3"))
    .withColumn("day", dayofmonth(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
    .withColumn("month", month(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
    .withColumn("year", year(to_date(unix_timestamp($"date", "dd/MM/yyyy").cast("timestamp"))))
  finalDayDf1.show(5)



}
