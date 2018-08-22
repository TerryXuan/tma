package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable

case class Test(id: Int, num: Array[Int])

object tmaUDF {
  def main(args: Array[String]): Unit = {
    val code1_lat_radiance = math.toRadians(6 + 17.0 / 60)
    val code2_lat_radiance = math.toRadians(6 + 50.0 / 60)
    val code1_lon_radiance = math.toRadians(73 + 1.0 / 60)
    val code2_lon_radiance = math.toRadians(73 + 2.0 / 60)
    println(CityCost.apply.calculatingDistance(code1_lon_radiance,code1_lat_radiance,code2_lon_radiance,code2_lat_radiance))
    println(CityCost.apply.calculatingDistance(73,2,6,50,73,1,6,17))
  }
}
