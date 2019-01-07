package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.{Accumulable, AccumulableParam, SparkConf, SparkContext}

object CustomAccumulator {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    implicit val param: AccumulableMap = new AccumulableMap()
    val empAccu: Accumulable[Map[Int, Employee], Employee] = sc.accumulable(Map[Int, Employee]())
    val employees: List[Employee] = List(
      Employee(10001, "Tom", "Eng"),
      Employee(10002, "Roger", "Sales"),
      Employee(10003, "Rafael", "Sales"))
    sc.parallelize(employees).foreach(e => empAccu += e)
    println("--------------Employees: ")
    empAccu.value.foreach(entry => println(s"emp id = ${entry._1} name = ${entry._2.name}"))

    sc.stop()
  }
}

case class Employee(id: Int, name: String, dep: String)

class AccumulableMap extends AccumulableParam[Map[Int, Employee], Employee] {

  override def addAccumulator(r: Map[Int, Employee], t: Employee): Map[Int, Employee] = r + (t.id -> t)

  override def addInPlace(r1: Map[Int, Employee], r2: Map[Int, Employee]): Map[Int, Employee] = r1 ++ r2

  override def zero(initialValue: Map[Int, Employee]): Map[Int, Employee] = Map[Int, Employee]()

}