package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.{Accumulable, AccumulableParam, SparkConf, SparkContext}

object CustomAccumulator {
  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    implicit val param: AccumulableMap = ???
    val empAccu: Accumulable[Map[Int, Employee], Employee] = ???

    val employees: List[Employee] = List(
      Employee(10001, "Tom", "Eng"),
      Employee(10002, "Roger", "Sales"),
      Employee(10003, "Rafael", "Sales"))

    sc.stop()
  }
}

case class Employee(id: Int, name: String, dep: String)

class AccumulableMap extends AccumulableParam[Map[Int, Employee], Employee] {

  override def addAccumulator(r: Map[Int, Employee], t: Employee): Map[Int, Employee] = r + (t.id -> t)

  override def addInPlace(r1: Map[Int, Employee], r2: Map[Int, Employee]): Map[Int, Employee] = r1 ++ r2

  override def zero(initialValue: Map[Int, Employee]): Map[Int, Employee] = Map[Int, Employee]()

}