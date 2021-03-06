package cn.sibat.tma

import scala.collection.mutable.ArrayBuffer

/**
  * 进化策略
  *
  * @param cities 城市集合
  * @author kong
  */
class EvolutionStrategy(cities: Array[CityTMA]) extends Serializable {
  //种群数
  private var population_size = 100
  //个体集合
  private val routes: ArrayBuffer[Array[Int]] = new ArrayBuffer[Array[Int]]()
  //迭代数
  private var iteration = 10000

  /**
    * 获取基因集合
    *
    * @return 基因集合
    */
  def getCities: Array[CityTMA] = cities

  /**
    * 获取迭代次数
    *
    * @return 迭代数
    */
  def getIteration: Int = iteration

  /**
    * 设置种群数量
    *
    * @param value 种群数
    */
  def setPopulationSize(value: Int): Unit = {
    population_size = value
  }

  /**
    * 设置迭代次数
    *
    * @param value 迭代数
    */
  def setIteration(value: Int): Unit = {
    iteration = value
  }

  /**
    * 初始化种群
    * 取基因的初始排列顺序的下标，后续计算都使用下标，节省内存
    * 随机打乱次数组成种群
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
    * 所有个体两两做交叉和变异
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
    * 所有个体两两做交叉和变异
    */
  def crossAndMutate1(): Unit = {
    val size = routes.length
    for (i <- routes.indices) {
      for (j <- routes.indices) {
        if (i != j) {
          val child = CrossStrategy.randomCross(routes(i), routes(j))
          val mutate = MutateStrategy.mutate(child._1, child._2)
          val cityCostP1 = CityCost.apply.cityCost(routes(i))
          val cityCostP2 = CityCost.apply.cityCost(routes(j))
          val cityCostC1 = CityCost.apply.cityCost(mutate._1)
          val cityCostC2 = CityCost.apply.cityCost(mutate._2)
          if (cityCostP1._1 > cityCostC1._1)
            routes(i) = mutate._1
          if (cityCostP2._1 > cityCostC2._1)
            routes(j) = mutate._2
        }
      }
    }
  }

  /**
    * 最优个体
    * 取适应度最小的个体
    */
  def bestOne(): Array[Int] = {
    routes.minBy(arr => CityCost.apply.cityCost(arr))
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
    val take = routes.sortBy(arr => CityCost.apply.cityCost(arr)).take(population_size)
    routes.clear()
    routes ++= take
  }

  /**
    * 新种群的构建算法
    * 选一半最优个体，一半随机生成个体
    */
  def selectPopulation1(): Unit = {
    val take = routes.sortBy(arr => CityCost.apply.cityCost(arr)).take(population_size / 2)
    routes.clear()
    routes ++= take
    for (i <- population_size / 2 until population_size) {
      routes += CrossStrategy.shuffle(take.head)
    }
  }
}
