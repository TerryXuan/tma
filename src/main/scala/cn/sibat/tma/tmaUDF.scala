package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Test(id: Int, num: Array[Int])

object tmaUDF {
  def main(args: Array[String]): Unit = {
    val a = Array(0,1,2)
    val b = Array(0,-1,1,-1,2)
    println(a.diff(b).isEmpty)
  }
}
