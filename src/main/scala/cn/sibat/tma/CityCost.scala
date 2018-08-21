package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable

/**
  * 自定义适应度函数
  *
  * @author kong
  */
object CityCost {
  //飞机的动态信息表,飞机code->info
  private var aircraft: mutable.HashMap[String, Aircraft] = _
  //酒店机场信息表
  private var airport: mutable.HashMap[String, Airport] = _

  /**
    * 设置飞机信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param aircraftMap 飞机信息
    */
  def setAircraft(aircraftMap: mutable.HashMap[String, Aircraft]): Unit = {
    aircraft = aircraftMap
  }

  /**
    * 设置机场或者酒店的信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param airportMap 酒店或机场的信息
    */
  def setLocation(airportMap: mutable.HashMap[String, Airport]): Unit = {
    airport = airportMap
  }

  /**
    * 设置所有的信息，包括飞机信息、机场酒店信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param aircraftMap 飞机信息
    * @param airportMap  机场酒店信息
    */
  def setAll(aircraftMap: mutable.HashMap[String, Aircraft], airportMap: mutable.HashMap[String, Airport]): Unit = {
    aircraft = aircraftMap
    airport = airportMap
  }

  /**
    * 计算两地之间距离
    * 单位是km
    *
    * @param code1_lon_radiance 地点1经度弧度制
    * @param code1_lat_radiance 地点1纬度弧度制
    * @param code2_lon_radiance 地点2经度弧度制
    * @param code2_lat_radiance 地点2纬度弧度制
    * @return dis
    */
  def calculatingDistance(code1_lon_radiance: Double, code1_lat_radiance: Double, code2_lon_radiance: Double, code2_lat_radiance: Double): Double = {
    math.acos(math.sin(code1_lat_radiance) * math.sin(code2_lat_radiance) + math.cos(code1_lat_radiance) * math.cos(code2_lat_radiance) * math.cos(code1_lon_radiance - code2_lon_radiance)) * 60.1126 / (math.Pi / 180)
  }

  /**
    * 计算两地之间距离
    * 单位是km
    *
    * @param longitudeDegr  地点1经度度数
    * @param longitudeMin   地点1经度分钟数
    * @param latitudeDegr   地点1纬度度数
    * @param latitudeMin    地点1纬度分钟数
    * @param longitudeDegr2 地点2经度度数
    * @param longitudeMin2  地点2经度分钟数
    * @param latitudeDegr2  地点2纬度度数
    * @param latitudeMin2   地点2纬度分钟数
    * @return dis
    */
  def calculatingDistance(longitudeDegr: Int, longitudeMin: Int, latitudeDegr: Int, latitudeMin: Int, longitudeDegr2: Int, longitudeMin2: Int, latitudeDegr2: Int, latitudeMin2: Int): Double = {
    val code1_lat_radiance = math.toRadians(latitudeDegr + latitudeMin / 60)
    val code2_lat_radiance = math.toRadians(latitudeDegr2 + latitudeMin2 / 60)
    val code1_lon_radiance = math.toRadians(longitudeDegr + longitudeMin / 60)
    val code2_lon_radiance = math.toRadians(longitudeDegr2 + longitudeMin2 / 60)
    calculatingDistance(code1_lon_radiance, code1_lat_radiance, code2_lon_radiance, code2_lat_radiance)
  }

  /**
    * 计算两地之间飞行所需的时间
    * 单位分钟m
    *
    * @param distance 距离
    * @return
    */
  def calculatingTime(distance: Double): Int = {
    if (distance == 0.0) 0 else math.round(distance / 2.25 + 2).toInt
  }

  def cityCost(cities: Array[CityTMA]): Double = {
    cities.length
  }

  /**
    * 适应度计算函数
    *
    * @param cities 基因集合
    * @param indexs 组合策略
    * @return
    */
  def cityCost(cities: Array[CityTMA], indexs: Array[Int]): Double = {
    /**
      * 限制条件
      * 1. 飞机日落半小时要落地。
      * 2. 飞机可飞时长为3小时和5小时，可略微超过该时间。
      * 3. 飞机负载，GTOW - APS = 行李重量 + 乘客重量 + 油重量
      * 4. 机场酒店距离和时间信息表
      * 5. 飞机停留时间，机场25min、酒店15min，加油+5min
      * 6. 酒店停留飞机数量限制，加油，排队
      * 7. 飞机续航里程限制，500lbs/h,油箱油量2000lbs。
      * 8. 中转限制
      * 9. 飞机数量限制，起始位置
      *
      */
    var cost = 0.0
    //用来标识d位置错位
    var temp = false
    val sdf = new SimpleDateFormat("H:mm:ss")

    for (i <- indexs) {
      val city = cities(i)
      if (city.odType.equals("o")) {
        //当前的任务
        val currentAirport = airport(city.name)

        //过滤不支持该任务的飞机
        val canUse = aircraft.filter(t => {
          //该任务的时间窗
          val st = sdf.parse(city.startTime).getTime
          val et = sdf.parse(city.endTime).getTime
          if (t._2.cities.isEmpty) { //空飞机
            true
          } else { //非空飞机
            // 当前飞机有无任务
            val cities = t._2.cities
            //第一个任务飞行距离与时间，即初始调度的耗费
            val aircraftAirport = airport(t._2.location)
            val firstAirport = airport(cities.head.name)
            val disAF = calculatingDistance(aircraftAirport.longitudeDegr, aircraftAirport.longitudeMin, aircraftAirport.latitudeDegr, aircraftAirport.latitudeMin, firstAirport.longitudeDegr, firstAirport.longitudeMin, firstAirport.latitudeDegr, firstAirport.latitudeMin)
            val firstTime = calculatingTime(disAF)

            //使用座位数
            val seats = cities.map(_.seats).sum

            // 已执行的飞行距离与时间
            var flyTime = 0
            var flyDis = 0.0
            for (i <- 0 until cities.length - 1) {
              val i_airport = airport(cities(i).name)
              val i_1_airport = airport(cities(i + 1).name)
              val dis = calculatingDistance(i_airport.longitudeDegr, i_airport.longitudeMin, i_airport.latitudeDegr, i_airport.latitudeMin, i_1_airport.longitudeDegr, i_1_airport.longitudeMin, i_1_airport.latitudeDegr, i_1_airport.latitudeMin)
              flyDis += dis
              flyTime += calculatingTime(dis)
            }

            //执行该任务所需的时间与距离
            val lastAirport = airport(cities.last.name)
            val currentDis = calculatingDistance(lastAirport.longitudeDegr, lastAirport.longitudeMin, lastAirport.latitudeDegr, lastAirport.latitudeMin, currentAirport.longitudeDegr, currentAirport.longitudeMin, currentAirport.latitudeDegr, currentAirport.latitudeMin)
            val currentTime = calculatingTime(currentDis)

            //座位满座，任务都执行完了，即没有时间窗限制，只需考虑飞行时间
            if (seats == 0) {
              // 当前飞机的位置，调度到该任务的位置和执行的时间，是否超过最大飞行时间
              t._2.maxFlyTime > firstTime + flyTime + currentTime
            } else { // 有任务,挑选未执行完的任务的时间窗
              val exec = cities.filter(c => c.odType.equals("d")).map(_.id)
              val t1 = cities.filter(c => !exec.contains(c.id)).maxBy(c => sdf.parse(c.startTime).getTime)
              val t2 = cities.filter(c => !exec.contains(c.id)).minBy(c => sdf.parse(c.endTime).getTime)
              math.max(sdf.parse(t1.startTime).getTime, sdf.parse(t2.endTime).getTime) > math.min(st, et) && t._2.maxFlyTime > firstTime + flyTime + currentTime && t._2.maxSeats >= seats + city.seats
            }
          }
        })

        //符合任务的飞机，挑选距离最小的飞机
        if (canUse.nonEmpty) {
          val near = canUse.minBy(t => {
            val a = airport(t._2.location)
            calculatingDistance(a.longitudeDegr, a.longitudeMin, a.latitudeDegr, a.latitudeMin, currentAirport.longitudeDegr, currentAirport.longitudeMin, currentAirport.latitudeDegr, currentAirport.latitudeMin)
          })._2
          val nearAirport = airport(near.location)
          val nearDistance = calculatingDistance(nearAirport.longitudeDegr, nearAirport.longitudeMin, nearAirport.latitudeDegr, nearAirport.latitudeMin, currentAirport.longitudeDegr, currentAirport.longitudeMin, currentAirport.latitudeDegr, currentAirport.latitudeMin)
          val nearTime = calculatingTime(nearDistance)
          aircraft.update(near.aircraftCode, near.copy(cities = near.cities ++ Array(city)))
        } else { // 无符合任务的飞机，调度不合理
          temp = true
        }
      } else {
        //该d的o已经上飞机，更新为下飞机，完成任务，飞机更新位置
        val existAircraft = aircraft.filter(t => t._2.cities.exists(c => c.id.equals(city.id)))
        if (existAircraft.nonEmpty) {
          val head = existAircraft.head
          aircraft.update(head._1, head._2.copy(cities = head._2.cities ++ Array(city)))
        } else //该d的o还没上飞机，属于非正常调度，直接reward0
          temp = true
      }
    }
    cost
  }

  /**
    * 采取行为后，环境的反馈
    *
    * @param S 状态
    * @param A 行为
    * @return (下一状态，奖励)
    */
  def get_env_feedback(S: CityTMA, A: CityTMA): (CityTMA, Double) = {
    var S_ = A
    var R = 0.0
    if (S.odType.equals("d")) {
      R = -1.0
      S_ = A
    } else if (S.odType.equals("o")) {
      R = 0.0
      S_ = A
    } else {

    }
    if (true) {
      S_ = CityTMA("end", "o", "end", "end", -1, -1, -1, -1, "end", "end")
      R = 1.0
    }
    (S_, R)
  }


}
