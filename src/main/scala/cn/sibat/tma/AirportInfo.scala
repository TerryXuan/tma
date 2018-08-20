package cn.sibat.tma

import org.apache.spark.sql.SparkSession

object AirportInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AirportInfo").master("local[*]").getOrCreate()
    import spark.implicits._
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

    airportInfo.show()
  }
}
