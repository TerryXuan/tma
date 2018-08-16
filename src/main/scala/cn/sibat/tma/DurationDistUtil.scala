package cn.sibat.tma

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DurationDistUtil {

  def dura_dist(data: DataFrame): DataFrame = {
    /**
      * 单位是km
      */
    val calculatingDistance = udf((code1: String, code2: String, code1_lon_radiance: Double, code1_lat_radiance: Double, code2_lon_radiance: Double, code2_lat_radiance: Double) => {
      math.acos(math.sin(code1_lat_radiance) * math.sin(code2_lat_radiance) + math.cos(code1_lat_radiance) * math.cos(code2_lat_radiance) * math.cos(code1_lon_radiance - code2_lon_radiance)) * 60.1126 / (math.Pi / 180)
    })

    /**
      * 单位是m
      */
    val calculatingTime = udf((distance: Double) => {
      if (distance == 0.0) 0.0 else distance / 2.25 + 2
    })

    val data1 = data.withColumn("LatRadiance", toRadians(col("LatitudeDegr") + col("LatitudeMin").divide(60))).withColumn("LonRadiance", toRadians(col("LongitudeDegr") + col("LongitudeMin").divide(60)))
      .withColumn("temp", lit("1"))
      .select("Code", "LatRadiance", "LonRadiance", "temp")

    val data2 = data1.toDF("Code2", "LatRadiance2", "LonRadiance2", "temp")

    data1.join(data2, "temp")
      .withColumn("distance", calculatingDistance(col("Code"), col("Code2"), col("LonRadiance"), col("LatRadiance"), col("LonRadiance2"), col("LatRadiance2")))
      .withColumn("time", calculatingTime(col("distance")))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val data = spark.read.option("header", value = true).csv("E:/data/TMA/Datasheet for 02052018 ver 1.4.csv")
    dura_dist(data).filter($"Code" === "MLE" && $"Code2" === "RIH").show(false)
    //dura_dist(data).repartition(1).write.mode("overwrite").csv("E:\\data\\TMA/dura_dist")
  }
}
