package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)
    thirdExample(sc)
    fourthExample(sc)

    sc.stop()
  }

  // lets first print out the contents of the RDD with partition labels
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    // This example returns 16 since the initial value is 5
    // reduce of partition 0 will be max(5, 1, 2, 3) = 5
    // reduce of partition 1 will be max(5, 4, 5, 6) = 6
    // final reduce across partitions will be 5 + 5 + 6 = 16
    // note the final reduce include the initial value

    val z: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6), 2)

    val spartitions = z.mapPartitionsWithIndex(myfunc).collect()

    val result0 = z.aggregate(0)(math.max(_,_), _ + _)
    val result5 = z.aggregate(5)(math.max(_,_), _ + _)

    println("-------- Example 1 --------")
    spartitions.foreach(println)
    println(s"-- ZeroValue: 0, Result: $result0")
    println(s"-- ZeroValue: 5, Result: $result5")
  }

  def secondExample(sc: SparkContext): Unit = {
    // See here how the initial value "x" is applied three times.
    //  - once for each partition
    //  - once when combining all the partitions in the second reduce function.

    val z: RDD[String] = sc.parallelize(List[String]("a","b","c","d","e","f"),2)

    val spartitions = z.mapPartitionsWithIndex(myfunc).collect()

    val result0 = z.aggregate("")(_+_, _+_)
    val resutlX = z.aggregate("x")(_+_, _+_)

    println("-------- Example 2 --------")
    spartitions.foreach(println)
    println(s"""-- ZeroValue: "", Result: $result0""")
    println(s"""-- ZeroValue: "x", Result: $resutlX""")
  }

  def thirdExample(sc: SparkContext): Unit = {
    val z = sc.parallelize(List("12","23","345","4567"),2)

    val spartitions = z.mapPartitionsWithIndex(myfunc).collect()

    val resultMax = z.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + " " + y)
    val resultMin = z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + " " + y)

    println("-------- Example 3 --------")
    spartitions.foreach(println)
    println(s"""-- ZeroValue: "", Max: $resultMax""")
    println(s"""-- ZeroValue: "", Min: $resultMin""")
  }

  def fourthExample(sc: SparkContext): Unit = {
    val z = sc.parallelize(List("12","23","345",""),2)

    val spartitions = z.mapPartitionsWithIndex(myfunc).collect()

    val resultMin = z.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + " " + y)

    println("-------- Example 4 --------")
    spartitions.foreach(println)
    println(s"""-- ZeroValue: "", Min: $resultMin""")
  }
}
