package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}

object FullOuterJoin {

  val fileAurl = "src/main/resources/join1_FileA.txt"
  val fileBurl = "src/main/resources/join1_FileB.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val fileA = sc.textFile(fileAurl)
    val fileB = sc.textFile(fileBurl)

    val fileA_data = fileA.map(split_fileA)
    val fileB_data = fileB.map(split_fileB)

    val fileB_joined_fileA = fileB_data.fullOuterJoin(fileA_data)
    val result = fileB_joined_fileA.collect().toList

    println(result)

    sc.stop()
  }

  def split_fileA(line: String): (String, Int) = {
    // split the input line in word and count on the comma
    val splitedLine = line.split(",")
    val word = splitedLine(0)
    // turn the count to an integer
    val count = splitedLine(1).toInt
    (word, count)
  }

  def split_fileB(line: String): (String, String) = {
    // split the input line into word, date and count_string
    val splitedLineByComma = line.split(",")
    val count_string = splitedLineByComma(1)
    val wordAndDate = splitedLineByComma(0)
    val splitedLineByBlank = wordAndDate.split(" ")
    val date = splitedLineByBlank(0)
    val word = splitedLineByBlank(1)
    (word, date + " " + count_string)
  }
}
