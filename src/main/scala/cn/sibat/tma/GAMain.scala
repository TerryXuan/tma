package cn.sibat.tma

import org.apache.spark.sql.SparkSession

object GAMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val cities = spark.read.option("header", value = true).csv("").as[CityTMA].collect()
    val data = spark.createDataFrame(Seq((1, 1), (1, 1), (1, 1), (1, 1)))
      .map(row => new Evolution(cities))

    val global = new BestCityAccumulate()
    data.foreach(e => {
      e.init()
      for (i <- 0 to e.getIteration) {
        if (CityCost.cityCost(e.getCities, e.bestOne()) < CityCost.cityCost(e.getCities, global.value)) {
          global.add(e.bestOne()) //出现最优个体放到全局变量中
        }
        e.crossAndMutate()
        e.selectPopulation()
      }
    })


  }
}
