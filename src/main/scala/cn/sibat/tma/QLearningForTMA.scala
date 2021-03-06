package cn.sibat.tma

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

/**
  * q-learning算法针对遗传算法版本
  *
  * @author kong
  */
object QLearningForTMA {
  var n_states = 9 //多少种基因
  var action: Array[Int] = (0 to 8).toArray //基因下标
  val epsilon = 0.9 //选择行为的概率，0.9代表90%的概率选择学习后的结果，10%的概率选择随机行为
  val learning_rate = 0.1 // 学习率 //bestOne:0.01+1000000 or 0.001+100000
  val gama = 0.9 //状态衰减率
  val max_learn = 10000000 //学习次数
  //基因序列
  private var bestOne: Array[Int] = _
  //初始化染色体
  private var globalCities: Array[(CityTMA, Int)] = _

  /**
    * 设置基因的信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param cities 基因的信息
    */
  def setCities(cities: Array[CityTMA]): Unit = {
    globalCities = cities.zipWithIndex
    bestOne = new Array[Int](cities.length).map(_ - 1)
    n_states = cities.length
    action = cities.indices.toArray
  }

  /**
    * 建立Q-table
    *
    * @param n_states 状态数
    * @param action   动作
    * @return q-table
    */
  def build_q_table(n_states: Int, action: Array[Int]): Array[Array[Double]] = {
    val table = Array.ofDim[Double](n_states, action.length)
    table
  }

  /**
    * 根据当前的状态和q-table，选择执行的行为
    *
    * @param state   当前状态
    * @param q_table q-table
    * @return 选择的行为
    */
  def choose_action(state: Int, q_table: Array[Array[Double]]): Int = {
    //1.当状态对应的行为的概率
    val state_actions = q_table(state)
    var action_name = -1
    //2. 选随机数 > epsilon 或者 处于初始化状态，随机选取行为
    if (math.random > epsilon || state_actions.forall(_ == 0.0)) {
      val choicef = action.diff(bestOne)
      if (choicef.length > 0) {
        val choice = new Random().nextInt(choicef.length)
        action_name = choicef(choice)
      }
    } else {
      //3. 非2的情况下，选取概率最大的行为
      val sorted = state_actions.clone().zipWithIndex.sortBy(_._1)
      var temp = sorted.map(_._2).diff(bestOne)
      action_name = sorted.filter(t => t._2 == temp.head).head._2
    }
    action_name
  }

  /**
    * 根据当前的状态和q-table，选择执行的行为
    *
    * @param state   当前状态
    * @param q_table q-table
    * @return 选择的行为
    */
  def choose_action1(state: Int, q_table: Array[Array[Double]]): Int = {
    //1.当状态对应的行为的概率
    val state_actions = q_table(state)
    var action_name = -1
    //2. 挑选可选集

    //2. 选随机数 > epsilon 或者 处于初始化状态，随机选取行为
    if (math.random > epsilon || state_actions.forall(_ == 0.0)) {
      val choicef = action.diff(bestOne)
      if (choicef.length > 0) {
        val choice = new Random().nextInt(choicef.length)
        action_name = choicef(choice)
      }
    } else {
      //3. 非2的情况下，选取概率最大的行为
      val sorted = state_actions.clone().zipWithIndex.sortBy(_._1)
      var temp = sorted.map(_._2).diff(bestOne)
      action_name = sorted.filter(t => t._2 == temp.head).head._2
    }
    action_name
  }

  /**
    * 根据当前的状态和q-table，选择执行的行为
    *
    * @param state   当前状态
    * @param q_table q-table
    * @return 选择的行为
    */
  def choose_action2(state: Int, q_table: Array[Array[Double]]): Int = {
    //1.当状态对应的行为的概率
    val state_actions = q_table(state)
    var action_name = -1
    //2. 选随机数 > epsilon 或者 处于初始化状态，随机选取行为
    if (math.random > epsilon || state_actions.forall(_ == 0.0)) {
      val choice = new Random().nextInt(action.length)
      action_name = action(choice)
    } else {
      //3. 非2的情况下，选取概率最大的行为
      val sorted = state_actions.clone().zipWithIndex.sortBy(_._1)
      action_name = sorted.maxBy(_._1)._2
      var temp = bestOne.contains(action_name)
      var count = sorted.length
      while (temp) {
        action_name = sorted(count - 1)._2
        temp = bestOne.contains(action_name)
        count -= 1
      }
    }
    action_name
  }

  /**
    * 采取行为后，环境的反馈
    *
    * @param S 状态
    * @param A 行为
    * @return (下一状态，奖励)
    */
  def get_env_feedback(cityCost: CityCost, S: Int, A: Int): (Int, Double) = {
    var S_ = A
    var R = 0.0
    if (bestOne.contains(-1)) {
      R = 0.0
      S_ = A
    }
    if (!bestOne.contains(-1)) {
      if (bestOne.distinct.length == bestOne.length) {
        S_ = -1
        val cost = cityCost.cityCost(bestOne)
        R = cost._1
      }
      //2. 行为左移，起点位置，左移还为起点，其他状态左移，不做奖励
    }
    (S_, R)
  }

  /**
    * 采取行为后，环境的反馈
    *
    * @param S 状态
    * @param A 行为
    * @return (下一状态，奖励)
    */
  def get_env_feedback2(cityCost: CityCost, S: Int, A: Int): (Int, Double) = {
    var S_ = A
    var R = 0.0
    if (bestOne.contains(-1)) {
      R = 0.0 //cityCost.cityCost(bestOne)._1
      S_ = A
      if (!bestOne.contains(S))
        bestOne(bestOne.indexOf(-1)) = S
    }
    if (!bestOne.contains(-1)) {
      if (bestOne.distinct.length == bestOne.length) {
        S_ = -1
        val cost = cityCost.cityCost(bestOne)
        //if (cost._1 > 0)
          R = cost._1
        //println(cost,bestOne.mkString(","))
      }
      //2. 行为左移，起点位置，左移还为起点，其他状态左移，不做奖励
    }
    (S_, R)
  }

  /**
    * 更新环境
    *
    * @param q_table      单前状态
    * @param episode      学习次数
    * @param step_counter 所需成本
    */
  def update_env(q_table: Array[Array[Double]], cityCost: CityCost, episode: Int, step_counter: Int): Unit = {
    val best_one = q_table.clone().map(s => {
      s.indexOf(s.max)
    })
    print(s"\rEpisode ${episode + 1}: total_steps = $step_counter,best one:${best_one.mkString(",")};")
    val cost = cityCost.cityCost(best_one)
    if (best_one.distinct.length == best_one.length && cost._1 > 0) {
      println("+++++++++++++++++++++++++++++++++++++++++++++++")
      println("learned best one!!!" + cost._2)
      println("-----------------------------------------------")
    }
    //Thread.sleep(100)
    //print("\r")
  }

  def resetBestOne(): Unit = {
    for (i <- bestOne.indices) {
      bestOne(i) = -1
    }
  }

  /**
    * 学习程序
    *
    * @return
    */
  def learn(aircraftMap: mutable.HashMap[String, Aircraft], airportMap: mutable.HashMap[String, Airport], cities: Array[CityTMA]): Array[Array[Double]] = {
    //1. 构建Q-table
    val q_table = build_q_table(n_states, action)
    //2. repeat
    for (epsilon <- 0 to max_learn) {
      var step_counter = 0
      val cityCost = new CityCost()
      cityCost.setAll(aircraftMap.clone(), airportMap.clone(), cities.clone())
      var S = new Random().nextInt(action.length)
      var is_terminated = false
      resetBestOne()
      //update_env(S, epsilon, step_counter)
      while (!is_terminated) {
        //3. loop:当前状态选择行为
        val A = choose_action2(S, q_table)
        //4. loop:当前状态和选择行为在环境中的反馈
        val next_states = get_env_feedback2(cityCost, S, A)
        //5.loop: 理论采取当前状态采取当前的行为的反馈q值
        val q_predict = q_table(S)(action.indexOf(A))
        //6. loop:实际上反馈的q值
        var q_target = 0.0
        if (next_states._1 != -1)
          q_target = next_states._2 + gama * q_table(next_states._1).max
        else {
          q_target = next_states._2
        }
        //7. loop:更新q-table
        q_table(S)(action.indexOf(A)) += learning_rate * (q_target - q_predict)
        S = next_states._1
        //update_env(S, epsilon, step_counter + 1)
        step_counter += 1
        if (S == -1)
          is_terminated = true
      }
      val cityCost1 = new CityCost()
      cityCost1.setAll(aircraftMap.clone(), airportMap.clone(), cities.clone())
      update_env(q_table, cityCost1, epsilon, step_counter)
    }
    q_table
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()
    import spark.implicits._
    val gene = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/gene1")

    val aircraft = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/aircraft.csv")
    val aircraftInfo = aircraft.map(row => {
      val code = row.getAs[String]("code")
      val location = row.getAs[String]("location")
      val minutes = row.getAs[Int]("Minutes")
      val gtow = row.getAs[Int]("GTOW")
      val seatCapacity = row.getAs[Int]("SeatCapacity")
      val aps = row.getAs[Int]("APS")
      Aircraft(code, gtow - aps, 0.0, minutes, seatCapacity, location, Array[CityTMA]())
    })

    val airport = spark.read.option("header", value = true).option("inferSchema", value = true).csv("E:/data/TMA/dataset/AirportCordinates.csv")
    val airportInfo = airport.map(row => {
      val code = row.getAs[String]("Code")
      val latitudeDegr = row.getAs[Int]("LatitudeDegr")
      val latitudeMin = row.getAs[Int]("LatitudeMin")
      val longitudeDegr = row.getAs[Int]("LongitudeDegr")
      val longitudeMin = row.getAs[Int]("LongitudeMin")
      val maxCapacity = row.getAs[Int]("AirportCapacity")
      val canOil = row.getAs[String]("FuelAvailable") match {
        case "No" => false
        case "Yes" => true
      }
      Airport(code, latitudeDegr, latitudeMin, longitudeDegr, longitudeMin, maxCapacity, canOil, 2, 0)
    })

    val aircraftMap = aircraftInfo.collect().map(a => (a.aircraftCode, a))
    val airportMap = airportInfo.collect().map(a => (a.airportCode, a))

    val aircraftHashMap = new mutable.HashMap[String, Aircraft]()
    aircraftHashMap ++= aircraftMap

    val airportHashMap = new mutable.HashMap[String, Airport]()
    airportHashMap ++= airportMap

    //染色体
    val chromosome = gene.as[CityTMA].collect().take(10)
    val indexs = chromosome.indices.toArray

    //(1.0672280726297556,22076.478363523987)

    setCities(chromosome)

    val qtable = learn(aircraftHashMap, airportHashMap, chromosome)
  }
}
