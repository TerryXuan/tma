package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object tmaUDF {
  def main(args: Array[String]): Unit = {
    val time = "9:02:00 AM"
    val sdf = new SimpleDateFormat("h:mm:ss")
    println(sdf.parse(time).getTime)
  }
}
