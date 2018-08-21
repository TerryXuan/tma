package cn.sibat.tma

/**
  * 基因编码
  * 即需求的信息
  *
  * @param id              id标识一个需求
  * @param odType          该基因类型,o or d
  * @param direction       乘客类型，depart or arrival
  * @param name            酒店名称
  * @param legs            最大中转数
  * @param seats           座位数
  * @param passengerWeight 乘客重量
  * @param baggageWeight   行李重量
  * @param startTime       时间窗开始
  * @param endTime         时间窗结束
  * @param dis             该任务所需的距离
  * @param time            该任务所需的时间
  */
case class CityTMA(id: String, odType: String, direction: String, name: String, legs: Int, seats: Int, passengerWeight: Double, baggageWeight: Double, startTime: String, endTime: String, dis: Double, time: Int)

/**
  * 飞机属性
  *
  * @param aircraftCode 飞机代码
  * @param maxWeight    最大载重
  * @param oilWeight    油量重量
  * @param maxFlyTime   最大飞行时间
  * @param maxSeats     最大座位
  * @param location     所处位置
  * @param cities       乘客
  */
case class Aircraft(aircraftCode: String, maxWeight: Double, oilWeight: Double, maxFlyTime: Int, maxSeats: Int, location: String, cities: Array[CityTMA])

/**
  * 酒店或者机场的信息
  *
  * @param airportCode   酒店或者机场代码
  * @param latitudeDegr  纬度degr
  * @param latitudeMin   纬度min
  * @param longitudeDegr 经度degr
  * @param longitudeMin  经度min
  * @param maxCapacity   最大停留数
  * @param canOil        是否能加油
  * @param legs          中转数
  * @param useCapacity   使用的机坪
  */
case class Airport(airportCode: String, latitudeDegr: Int, latitudeMin: Int, longitudeDegr: Int, longitudeMin: Int, maxCapacity: Int, canOil: Boolean, legs: Int, useCapacity: Int)