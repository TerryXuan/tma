package cn.sibat.tma

import java.text.SimpleDateFormat

import scala.collection.mutable

object SelectStrategy {
  //飞机的动态信息表,飞机code->info
  private var aircraft: mutable.HashMap[String, Aircraft] = _
  //酒店机场信息表
  private var airport: mutable.HashMap[String, Airport] = _

  //初始化染色体
  private var globalCities: Array[CityTMA] = _

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
    * 设置基因的信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param cities 基因的信息
    */
  def setCities(cities: Array[CityTMA]): Unit = {
    globalCities = cities
  }

  /**
    * 设置所有的信息，包括飞机信息、机场酒店信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param aircraftMap 飞机信息
    * @param airportMap  机场酒店信息
    * @param cities      基因的信息
    */
  def setAll(aircraftMap: mutable.HashMap[String, Aircraft], airportMap: mutable.HashMap[String, Airport], cities: Array[CityTMA]): Unit = {
    aircraft = aircraftMap
    airport = airportMap
    globalCities = cities
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
    val code1_lat_radiance = math.toRadians(latitudeDegr + latitudeMin / 60.0)
    val code2_lat_radiance = math.toRadians(latitudeDegr2 + latitudeMin2 / 60.0)
    val code1_lon_radiance = math.toRadians(longitudeDegr + longitudeMin / 60.0)
    val code2_lon_radiance = math.toRadians(longitudeDegr2 + longitudeMin2 / 60.0)
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

  /**
    * 适应度计算函数
    *
    * @param indexs 组合策略
    * @return
    */
  def cityCost(indexs: String): (Double, Double) = {
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
    val sdf = new SimpleDateFormat("H:mm:ss")
    val split = indexs.split("-1")
    //按出发时间排列任务
    val sortByTime = split.sortBy(s => {
      s.split(",").maxBy(c => sdf.parse(globalCities(c.toInt).startTime).getTime)
    })

    for (plan <- sortByTime) {
      val indexs = plan.split(",").map(_.toInt)
      val cityTMAs = indexs.map(i => globalCities(i))

      if (cityTMAs.head.direction.equals("Departing")) {
        val sortedSetO = cityTMAs.filter(c => c.odType.equals("o")).sortBy(_.legs)
        val sortedSetD = cityTMAs.filter(c => c.odType.equals("d")) //终点都是马内，所以不考虑
        val startCities = sortedSetO.filter(c => c.legs == sortedSetO.head.legs) //起始的legs
        //当前的任务起点集合，可能相同，可能不同，比如A的legs是1，B的legs是1，实际执行的时候A的legs为0
        val currentAirports = startCities.map(c => airport(c.name)).distinct
        //该任务的时间窗
        val st = sdf.parse(startCities.maxBy(c => sdf.parse(c.startTime).getTime).startTime).getTime
        val et = sdf.parse(startCities.minBy(c => sdf.parse(c.endTime).getTime).endTime).getTime

        //过滤不支持该任务的飞机
        val canUse = aircraft.filter(t => {
          if (t._2.cities.isEmpty) { //空飞机
            true
          } else { //非空飞机
            // 当前飞机有无任务
            val cities1 = t._2.cities

            //执行该任务所需的时间与距离
            val lastAirport = airport(cities1.last.name)
            val currentAirport = currentAirports.minBy(p => calculatingDistance(lastAirport.longitudeDegr, lastAirport.longitudeMin, lastAirport.latitudeDegr, lastAirport.latitudeMin, p.longitudeDegr, p.longitudeMin, p.latitudeDegr, p.latitudeMin))
            val currentDis = calculatingDistance(lastAirport.longitudeDegr, lastAirport.longitudeMin, lastAirport.latitudeDegr, lastAirport.latitudeMin, currentAirport.longitudeDegr, currentAirport.longitudeMin, currentAirport.latitudeDegr, currentAirport.latitudeMin)
            val currentTime = calculatingTime(currentDis)

            // 当前飞机的位置，调度到该任务的位置和执行的时间，是否超过最大飞行时间
            val exec = cities1.filter(c => c.odType.equals("d")).map(_.id)
            val remain = cities1.filter(c => !exec.contains(c.id))
            if (remain.isEmpty)
              false
            else {
              val t1 = remain.maxBy(c => sdf.parse(c.startTime).getTime)
              val t2 = remain.minBy(c => sdf.parse(c.endTime).getTime)
              math.max(sdf.parse(t1.startTime).getTime, sdf.parse(t2.endTime).getTime) > math.min(st, et) && t._2.maxFlyTime > firstTime + flyTime + currentTime + city.time && t._2.maxSeats >= seats + city.seats
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
        }
      } else {
        //该d的o已经上飞机，更新为下飞机，完成任务，飞机更新位置
        //        val existAircraft = aircraft.filter(t => t._2.cities.exists(c => c.id.equals(city.id)))
        //        if (existAircraft.nonEmpty) {
        //          val head = existAircraft.head
        //          aircraft.update(head._1, head._2.copy(cities = head._2.cities ++ Array(city)))
        //        }
      }
    }
    var cost = 0.0
    var total = 0.0

    aircraft.foreach(t => {
      val cityTMAs = t._2.cities
      if (cityTMAs.nonEmpty) {
        val aircraftAirport = airport(t._2.location)
        val firstAirport = airport(cityTMAs.head.name)
        val disAF = calculatingDistance(aircraftAirport.longitudeDegr, aircraftAirport.longitudeMin, aircraftAirport.latitudeDegr, aircraftAirport.latitudeMin, firstAirport.longitudeDegr, firstAirport.longitudeMin, firstAirport.latitudeDegr, firstAirport.latitudeMin)

        // 已执行的飞行距离与时间
        var flyDis = 0.0
        for (i <- 0 until cityTMAs.length - 1) {
          val i_airport = airport(cityTMAs(i).name)
          val i_1_airport = airport(cityTMAs(i + 1).name)
          val dis = calculatingDistance(i_airport.longitudeDegr, i_airport.longitudeMin, i_airport.latitudeDegr, i_airport.latitudeMin, i_1_airport.longitudeDegr, i_1_airport.longitudeMin, i_1_airport.latitudeDegr, i_1_airport.latitudeMin)
          flyDis += dis
        }
        total += (flyDis + disAF)
      }
    })
    cost = if (total != 0.0) 1 / total else 0.0
    //cost = if (total != 0.0) 1/total else 0.0
    (cost, total)
  }

  /**
    * 城市的索引
    *
    * @param indexs indexs
    */
  def save(indexs: Array[Int]): Unit = {
    val cities = indexs.map(globalCities(_))
    val depart = cities.filter(c => c.direction.equals("Departing"))
    val arrival = cities.filter(c => c.direction.equals("Arriving"))
    val toDepartAircraft = aircraft.filter(t => !t._2.location.equals("MLE"))
    val sdf = new SimpleDateFormat("H:mm:ss")
    val hasAircraftAirport = toDepartAircraft.map(_._2.location)


    for (city <- depart) {
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
            val cities1 = t._2.cities
            //第一个任务飞行距离与时间，即初始调度的耗费
            val aircraftAirport = airport(t._2.location)
            val firstAirport = airport(cities1.head.name)
            val disAF = calculatingDistance(aircraftAirport.longitudeDegr, aircraftAirport.longitudeMin, aircraftAirport.latitudeDegr, aircraftAirport.latitudeMin, firstAirport.longitudeDegr, firstAirport.longitudeMin, firstAirport.latitudeDegr, firstAirport.latitudeMin)
            val firstTime = calculatingTime(disAF)

            //使用座位数
            val seats = cities1.map(_.seats).sum

            // 已执行的飞行距离与时间
            var flyTime = 0
            var flyDis = 0.0
            for (i <- 0 until cities1.length - 1) {
              val i_airport = airport(cities1(i).name)
              val i_1_airport = airport(cities1(i + 1).name)
              val dis = calculatingDistance(i_airport.longitudeDegr, i_airport.longitudeMin, i_airport.latitudeDegr, i_airport.latitudeMin, i_1_airport.longitudeDegr, i_1_airport.longitudeMin, i_1_airport.latitudeDegr, i_1_airport.latitudeMin)
              flyDis += dis
              flyTime += calculatingTime(dis)
            }

            //执行该任务所需的时间与距离
            val lastAirport = airport(cities1.last.name)
            val currentDis = calculatingDistance(lastAirport.longitudeDegr, lastAirport.longitudeMin, lastAirport.latitudeDegr, lastAirport.latitudeMin, currentAirport.longitudeDegr, currentAirport.longitudeMin, currentAirport.latitudeDegr, currentAirport.latitudeMin)
            val currentTime = calculatingTime(currentDis)

            //座位满座，任务都执行完了，即没有时间窗限制，只需考虑飞行时间
            if (seats == 0) {
              // 当前飞机的位置，调度到该任务的位置和执行的时间，是否超过最大飞行时间
              t._2.maxFlyTime > firstTime + flyTime + currentTime + city.time
            } else { // 有任务,挑选未执行完的任务的时间窗
              val exec = cities1.filter(c => c.odType.equals("d")).map(_.id)
              val remain = cities1.filter(c => !exec.contains(c.id))
              if (remain.isEmpty)
                false
              else {
                val t1 = remain.maxBy(c => sdf.parse(c.startTime).getTime)
                val t2 = remain.minBy(c => sdf.parse(c.endTime).getTime)
                math.max(sdf.parse(t1.startTime).getTime, sdf.parse(t2.endTime).getTime) > math.min(st, et) && t._2.maxFlyTime > firstTime + flyTime + currentTime + city.time && t._2.maxSeats >= seats + city.seats
              }
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
        } else
          punishCount += 1
      } else {
        //该d的o已经上飞机，更新为下飞机，完成任务，飞机更新位置
        val existAircraft = aircraft.filter(t => t._2.cities.exists(c => c.id.equals(city.id)))
        if (existAircraft.nonEmpty) {
          val head = existAircraft.head
          aircraft.update(head._1, head._2.copy(cities = head._2.cities ++ Array(city)))
        } else
          punishCount += 1
      }
    }
    var cost = 0.0
    var total = 0.0

    aircraft.foreach(t => {
      val cityTMAs = t._2.cities
      if (cityTMAs.nonEmpty) {
        val aircraftAirport = airport(t._2.location)
        val firstAirport = airport(cityTMAs.head.name)
        val disAF = calculatingDistance(aircraftAirport.longitudeDegr, aircraftAirport.longitudeMin, aircraftAirport.latitudeDegr, aircraftAirport.latitudeMin, firstAirport.longitudeDegr, firstAirport.longitudeMin, firstAirport.latitudeDegr, firstAirport.latitudeMin)

        // 已执行的飞行距离与时间
        var flyDis = 0.0
        for (i <- 0 until cityTMAs.length - 1) {
          val i_airport = airport(cityTMAs(i).name)
          val i_1_airport = airport(cityTMAs(i + 1).name)
          val dis = calculatingDistance(i_airport.longitudeDegr, i_airport.longitudeMin, i_airport.latitudeDegr, i_airport.latitudeMin, i_1_airport.longitudeDegr, i_1_airport.longitudeMin, i_1_airport.latitudeDegr, i_1_airport.latitudeMin)
          flyDis += dis
        }
        total += (flyDis + disAF)
      }
    })
    if (punishCount > 0)
      cost = -punishCount
    else
      cost = if (total != 0.0) 1 / total else -1.0
    //cost = if (total != 0.0) 1/total else 0.0
    (cost, total)
  }
}
