package cn.sibat.tma

import scala.collection.mutable

/**
  * 自定义适应度函数
  *
  * @author kong
  */
object CityCost {
  //飞机的动态信息表,飞机code->info
  private var aircraft: mutable.HashMap[String, String] = _
  //od信息表，od-> info
  private var dura_dist: mutable.HashMap[String, String] = _
  //酒店机场信息表
  private var location: mutable.HashMap[String, String] = _

  /**
    * 设置飞机信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param aircraftMap 飞机信息
    */
  def setAircraft(aircraftMap: mutable.HashMap[String, String]): Unit = {
    aircraft = aircraftMap
  }

  /**
    * 设置酒店之间的距离信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param duraDist 酒店距离信息
    */
  def setDuraDist(duraDist: mutable.HashMap[String, String]): Unit = {
    dura_dist = duraDist
  }

  /**
    * 设置机场或者酒店的信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param locationMap 酒店或机场的信息
    */
  def setLocation(locationMap: mutable.HashMap[String, String]): Unit = {
    location = locationMap
  }

  /**
    * 设置所有的信息，包括飞机信息、酒店之间距离信息、机场酒店信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param aircraftMap 飞机信息
    * @param duraDist    酒店之间距离信息
    * @param locationMap 机场酒店信息
    */
  def setAll(aircraftMap: mutable.HashMap[String, String], duraDist: mutable.HashMap[String, String], locationMap: mutable.HashMap[String, String]): Unit = {
    aircraft = aircraftMap
    dura_dist = duraDist
    location = locationMap
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
    cities.length
  }

}
