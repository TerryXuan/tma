package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object tmaUDF {
  def main(args: Array[String]): Unit = {
    val time = "19:02:00"
    val sdf = new SimpleDateFormat("H:mm:ss")
    val h = sdf.parse(time).getTime - 30 * 60 * 1000
    println(sdf.format(h))
  }
}
