package cn.sibat.test

import java.text.SimpleDateFormat

import cn.sibat.tma._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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
      val longitudeDegr = row.getAs[Int]("LongitudeDegr")
      val longitudeMin = row.getAs[Int]("LongitudeMin")
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

    val chromosomeWithIndex = chromosome.zipWithIndex

    //构建新的染色体，不以随机形式，保证染色体可用，才可评估优劣
    val resultIndex = new ArrayBuffer[Int]()
    val ran = new Random()
    val sdf = new SimpleDateFormat("H:mm:ss")
    while (indexs.diff(resultIndex).nonEmpty) {
      if (resultIndex.isEmpty) { // 刚开始随机选择出发点必须为o类型
        val filter = chromosome.filter(c => c.odType.equals("o"))
        val ranIndex = ran.nextInt(filter.length)
        resultIndex += chromosome.indexOf(filter(ranIndex))
        //满最大飞机容量，不能拼单
        if (filter(ranIndex).seats == 16) {
          resultIndex += chromosome.indexOf(chromosome.filter(f => f.odType.equals("d") && f.id.equals(filter(ranIndex).id)).head)
          resultIndex += -1
        }
      } else {
        val firstWindow = resultIndex.slice(resultIndex.lastIndexOf(-1) + 1, resultIndex.length) //当前的任务前面执行集合
        //当前还没有分配任务,随机重新分配未被选取的o
        if (firstWindow.isEmpty) {
          val filter = chromosomeWithIndex.filter(c => c._1.odType.equals("o"))
          val noSelectFilter = filter.map(_._2).diff(resultIndex)
          val ranIndex = ran.nextInt(noSelectFilter.length)
          val index = filter.find(t => t._2 == noSelectFilter(ranIndex)).get._2
          resultIndex += index
          if (chromosome(index).seats == 16) {
            resultIndex += chromosome.indexOf(chromosome.filter(f => f.odType.equals("d") && f.id.equals(chromosome(index).id)).head)
            resultIndex += -1
          }
        } else {
          // 1. 当前的任务集legs已满不可再加任务
          val currentTask = chromosomeWithIndex.filter(t => firstWindow.contains(t._2))

          //已使用座位
          val seats = currentTask.map(_._1.seats).sum

          // 剩下的任务
          val noExecTask = chromosomeWithIndex.filter(t => !resultIndex.contains(t._2))
          val legsMax = 2 //最大的legs是2
          val existCityLength = currentTask.map(_._1.name).distinct.length - 2 // 当前的legs
          val t1 = currentTask.maxBy(c => sdf.parse(c._1.startTime).getTime) //当前最晚出发时间
          val sameTypeTask = noExecTask.filter(t => {
            val c_t2 = sdf.parse(t._1.endTime).getTime
            val t_t1 = sdf.parse(t1._1.startTime).getTime
            t._1.direction.equals(currentTask.head._1.direction) && t_t1 < c_t2 && t._1.seats + seats <= 16
          })
          if (legsMax == existCityLength) {
            //同酒店同时间窗，随机选
            val sameAirport = sameTypeTask.filter(t => currentTask.map(_._1.name).contains(t._1.name))
            if (sameAirport.isEmpty)
              resultIndex += -1
            else
              resultIndex += 1
          } else { //2. legs未满，可加同类任务
            //当前窗口时间限制
            resultIndex += 1
          }
        }
      }
    }

    //(1.0672280726297556,22076.478363523987)
    var best = indexs
    var bestCost = 0.0
    for (i <- 0 to 100000) {
      val costCity = new CityCost()
      costCity.setAll(aircraftHashMap.clone(), airportHashMap.clone(), chromosome.clone())
      val index = CrossStrategy.shuffle(indexs)
      val cost = costCity.cityCost(index)
      if (cost._1 > 0.0) {
        println(index.mkString(","))
      }
      if (bestCost < cost._1) {
        bestCost = cost._1
        best = index
      }
    }

    println(best.mkString(",") + ";" + bestCost)
  }
}
