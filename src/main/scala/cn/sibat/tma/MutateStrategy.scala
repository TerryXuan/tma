package cn.sibat.tma

import scala.util.Random

/**
  * 变异策略
  *
  * @author kong
  */
object MutateStrategy {

  /**
    * 直接交换某两个基因
    *
    * @param route1 route1
    * @param route2 route2
    */
  def mutate(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val ran = new Random()
    val mu1 = ran.nextInt(route1.length)
    val mu2 = ran.nextInt(route1.length)
    var temp = route1(mu1)
    route1(mu1) = route1(mu2)
    route1(mu2) = temp
    temp = route2(mu1)
    route2(mu1) = route2(mu2)
    route2(mu2) = temp
    (route1, route2)
  }

}
