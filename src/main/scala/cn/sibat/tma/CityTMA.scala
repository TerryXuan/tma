package cn.sibat.tma

/**
  * 基因编码
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
  */
case class CityTMA(id: String, odType: String, direction: String, name: String, legs: Int, seats: Int, passengerWeight: Double, baggageWeight: Double, startTime: String, endTime: String)
