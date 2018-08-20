package cn.sibat.tma

import org.apache.spark.sql.SparkSession

object AircraftInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AircraftInfo").master("local[*]").getOrCreate()
    import spark.implicits._
    val aircraft = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/aircraft.csv")
    val aircraftInfo = aircraft.map(row => {
      val code = row.getAs[String]("code")
      val location = row.getAs[String]("location")
      val minutes = row.getAs[Int]("Minutes")
      val gtow = row.getAs[Int]("GTOW")
      val seatCapacity = row.getAs[Int]("SeatCapacity")
      val aps = row.getAs[Int]("APS")
      Aircraft(code, "start", gtow - aps, 0.0, minutes, seatCapacity, 0, location, 0.0, 0.0)
    })

    aircraftInfo.show()
  }
}
