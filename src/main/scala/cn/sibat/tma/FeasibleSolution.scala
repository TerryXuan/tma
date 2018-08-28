package cn.sibat.tma

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object FeasibleSolution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FeasibleSolution").getOrCreate()
    val inputPath = args(0)
    val outputPath = args(1)
    import spark.implicits._
    val gene = spark.read.option("header", value = true).option("inferSchema", value = true).csv(inputPath)
    //染色体
    val chromosome = gene.as[CityTMA].collect()
    val indexs = chromosome.indices.toArray

    val chromosomeWithIndex = chromosome.zipWithIndex

    val bChromosome = spark.sparkContext.broadcast(chromosomeWithIndex)

    spark.sparkContext.parallelize(1 to 10000, numSlices = 200).flatMap(i => {
      val chromosomeWI = bChromosome.value
      val oType = chromosomeWI.filter(t => t._1.odType.equals("o"))
      val dType = chromosomeWI.filter(t => t._1.odType.equals("d"))

      //构建新的染色体，不以随机形式，保证染色体可用，才可评估优劣
      val rdd = new ArrayBuffer[String]()
      for (i <- 0 to 100) {
        val resultIndex = new ArrayBuffer[Int]()
        val ran = new Random()
        val sdf = new SimpleDateFormat("H:mm:ss")
        while (indexs.diff(resultIndex).nonEmpty) {
          if (resultIndex.isEmpty) { // 刚开始随机选择出发点必须为o类型
            val ranIndex = ran.nextInt(oType.length)
            resultIndex += oType(ranIndex)._2
            resultIndex += dType.filter(f => f._1.id.equals(oType(ranIndex)._1.id)).head._2
            //满最大飞机容量，不能拼单
            if (oType(ranIndex)._1.seats == 16) {
              resultIndex += -1
            }
          } else {
            val firstWindow = resultIndex.slice(resultIndex.lastIndexOf(-1) + 1, resultIndex.length) //当前的任务前面执行集合
            //当前还没有分配任务,随机重新分配未被选取的o
            if (firstWindow.isEmpty) {
              val noSelectFilter = oType.map(_._2).diff(resultIndex)
              val ranIndex = ran.nextInt(noSelectFilter.length)
              val index = oType.find(t => t._2 == noSelectFilter(ranIndex)).get
              resultIndex += index._2
              resultIndex += dType.filter(f => f._1.id.equals(index._1.id)).head._2
              if (index._1.seats == 16) {
                resultIndex += -1
              }
            } else {
              // 1. 当前的任务集legs已满不可再加任务
              val currentTaskO = oType.filter(t => firstWindow.contains(t._2))
              val currentTaskD = dType.filter(t => firstWindow.contains(t._2))

              //已使用座位
              val seats = currentTaskO.map(_._1.seats).sum

              // 剩下的任务
              val noExecTask = chromosomeWithIndex.filter(t => !resultIndex.contains(t._2))
              val legsMax = 2 //最大的legs是2
              val existCityLength = currentTaskO.map(_._1.name).distinct.length + currentTaskD.map(_._1.name).distinct.length - 2 // 当前的legs
              val t1 = currentTaskO.maxBy(c => sdf.parse(c._1.endTime).getTime) //当前最晚出发时间
              val sameTypeTaskO = noExecTask.filter(t => {
                val c_t2 = sdf.parse(t._1.startTime).getTime
                val t_t1 = sdf.parse(t1._1.endTime).getTime
                t._1.direction.equals(currentTaskO.head._1.direction) && t_t1 < c_t2 && t._1.seats + seats <= 16 && t._1.odType.equals("o")
              })
              if (sameTypeTaskO.isEmpty) {
                resultIndex += -1
              } else {
                if (legsMax == existCityLength) {
                  //同酒店同时间窗，随机选
                  val currentName = currentTaskD.map(_._1.name) ++ currentTaskO.map(_._1.name)
                  val ids = noExecTask.groupBy(_._1.id).filter(t => t._2.map(_._1.name).diff(currentName).isEmpty).keys.toArray
                  val sameAirport = sameTypeTaskO.filter(t => ids.contains(t._1.id))
                  if (sameAirport.isEmpty)
                    resultIndex += -1
                  else {
                    val ranIndex = ran.nextInt(sameAirport.length)
                    val index = sameAirport(ranIndex)
                    resultIndex += index._2
                    resultIndex += dType.filter(f => f._1.id.equals(index._1.id)).head._2
                  }
                } else { //2. legs未满，可加同类任务
                  //当前窗口时间限制
                  val ranIndex = ran.nextInt(sameTypeTaskO.length)
                  val index = sameTypeTaskO(ranIndex)
                  resultIndex += index._2
                  resultIndex += dType.filter(f => f._1.id.equals(index._1.id)).head._2
                }
              }
            }
          }
        }
        rdd += resultIndex.mkString(",")
      }
      rdd
    }).toDF().write.mode("append").csv(outputPath)
  }
}
