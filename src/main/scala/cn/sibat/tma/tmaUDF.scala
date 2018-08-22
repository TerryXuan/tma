package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable

case class Test(id: Int, num: Array[Int])

object tmaUDF {
  def main(args: Array[String]): Unit = {
    val dd = Array(1,2,3,4,5)
    val d = Array(1,5)
    for (i<- dd.indices){
      dd(i) *= 1 * 0
    }
    println(dd.mkString(","))
  }
}
