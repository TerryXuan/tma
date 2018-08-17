package cn.sibat.tma

import scala.collection.mutable.ArrayBuffer

class EvolutionStrategy(cities: Array[CityTMA]) extends Serializable {
  private var population_size = 100
  private val routes: ArrayBuffer[Array[Int]] = new ArrayBuffer[Array[Int]]()
  private var iteration = 10000

  def getCities: Array[CityTMA] = cities

  def getIteration: Int = iteration

  def setPopulationSize(value: Int): Unit = {
    population_size = value
  }

  def setIteration(value: Int): Unit = {
    iteration = value
  }

  /**
    * 初始化种群
    */
  def init(): Unit = {
    val zeros = cities.indices.toArray
    routes += zeros
    for (i <- 1 until routes.length) {
      routes += CrossStrategy.shuffle(zeros)
    }
  }

  /**
    * 无淘汰制
    * 父体和子体全保留，后面选top N
    */
  def crossAndMutate(): Unit = {
    val size = routes.length
    for (i <- routes.indices) {
      for (j <- routes.indices) {
        if (i != j) {
          val child = CrossStrategy.randomCross(routes(i), routes(j))
          val mutate = MutateStrategy.mutate(child._1, child._2)
          routes += mutate._1
          routes += mutate._2
        }
      }
    }
  }

  /**
    * 淘汰制
    * 每次交叉都只保留两个最优个体
    */
  def crossAndMutate1(): Unit = {
    val size = routes.length
    for (i <- routes.indices) {
      for (j <- routes.indices) {
        if (i != j) {
          val child = CrossStrategy.randomCross(routes(i), routes(j))
          val mutate = MutateStrategy.mutate(child._1, child._2)
          val cityCostP1 = CityCost.cityCost(cities, routes(i))
          val cityCostP2 = CityCost.cityCost(cities, routes(j))
          val cityCostC1 = CityCost.cityCost(cities, mutate._1)
          val cityCostC2 = CityCost.cityCost(cities, mutate._2)
          if (cityCostP1 > cityCostC1)
            routes(i) = mutate._1
          if (cityCostP2 > cityCostC2)
            routes(j) = mutate._2
        }
      }
    }
  }

  /**
    * 最优个体
    */
  def bestOne(): Array[Int] = {
    routes.minBy(arr => CityCost.cityCost(cities, arr))
  }

  /**
    * 从全局变量添加到种群中
    */
  def addBestOne(bestOne: Array[Int]): Unit = {
    if (!routes.contains(bestOne))
      routes += bestOne
  }

  /**
    * 新种群的构建算法
    * 选取最优的前种群数
    */
  def selectPopulation(): Unit = {
    val take = routes.sortBy(arr => CityCost.cityCost(cities, arr)).take(population_size)
    routes.clear()
    routes ++= take
  }

  /**
    * 新种群的构建算法
    * 选一半最优个体，一半随机生成个体
    */
  def selectPopulation1(): Unit = {
    val take = routes.sortBy(arr => CityCost.cityCost(cities, arr)).take(population_size / 2)
    routes.clear()
    routes ++= take
    for (i <- population_size / 2 until population_size) {
      routes += CrossStrategy.shuffle(take.head)
    }
  }
}
