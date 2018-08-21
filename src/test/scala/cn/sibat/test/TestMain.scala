package cn.sibat.test

import cn.sibat.tma.{Aircraft, Airport, CityCost, CityTMA}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object TestMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()
    import spark.implicits._
    val gene = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/gene1")

    val aircraft = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/aircraft.csv")
    val aircraftInfo = aircraft.map(row => {
      val code = row.getAs[String]("code")
      val location = row.getAs[String]("location")
      val minutes = row.getAs[Int]("Minutes")
      val gtow = row.getAs[Int]("GTOW")
      val seatCapacity = row.getAs[Int]("SeatCapacity")
      val aps = row.getAs[Int]("APS")
      Aircraft(code, gtow - aps, 0.0, minutes, seatCapacity, location, Array[CityTMA]())
    })

    val airport = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/AirportCordinates.csv")
    val airportInfo = airport.map(row => {
      val code = row.getAs[String]("Code")
      val latitudeDegr = row.getAs[Int]("LatitudeDegr")
      val latitudeMin = row.getAs[Int]("LatitudeMin")
      val longitudeDegr = row.getAs[Int]("LatitudeDegr")
      val longitudeMin = row.getAs[Int]("LatitudeMin")
      val maxCapacity = row.getAs[Int]("AirportCapacity")
      val canOil = row.getAs[String]("FuelAvailable") match {
        case "No" => false
        case "Yes" => true
      }
      Airport(code, latitudeDegr, latitudeMin, longitudeDegr, longitudeMin, maxCapacity, canOil, 2, 0)
    })

    val aircraftMap = aircraftInfo.collect().map(a => (a.aircraftCode, a))
    val airportMap = airportInfo.collect().map(a => (a.airportCode, a))

    val aircraftHashMap = new mutable.HashMap[String, Aircraft]()
    aircraftHashMap ++= aircraftMap

    val airportHashMap = new mutable.HashMap[String, Airport]()
    airportHashMap ++= airportMap

    //染色体
    val chromosome = gene.as[CityTMA].collect()
    val indexs = chromosome.indices.toArray

    //(1.0672280726297556,22076.478363523987)
  }
}
