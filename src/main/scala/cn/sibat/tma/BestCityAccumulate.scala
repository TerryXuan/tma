package cn.sibat.tma

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
  * 自定义全局最优序列
  * 用序列值替换City，优化内存，提高速度
  *
  * @author kong
  */
class BestCityAccumulate extends AccumulatorV2[Array[Int], Array[Int]] {
  private val cities = new ArrayBuffer[Int]()

  override def isZero: Boolean = cities.isEmpty

  override def copy(): AccumulatorV2[Array[Int], Array[Int]] = {
    val newCities = new BestCityAccumulate()
    cities.synchronized(newCities.cities.appendAll(cities))
    newCities
  }

  override def reset(): Unit = cities.clear()

  /**
    * 只做替换
    * 成本比较交给executor
    *
    * @param v 替换目标v
    */
  override def add(v: Array[Int]): Unit = {
    //保留成本最优
    cities.clear()
    cities.appendAll(v)
  }

  override def merge(other: AccumulatorV2[Array[Int], Array[Int]]): Unit = {
    other match {
      case city: BestCityAccumulate =>
        cities.clear()
        cities.appendAll(other.value)
    }
  }

  override def value: Array[Int] = cities.toArray
}
