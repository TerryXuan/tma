package cn.sibat.tma

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

object SarsaLambda {
  var n_states = 9 //多少种基因
  var action: Array[Int] = (0 to 8).toArray //基因下标
  val epsilon = 0.8 //选择行为的概率，0.9代表90%的概率选择学习后的结果，10%的概率选择随机行为
  val learning_rate = 0.01 // 学习率 //bestOne:0.01+1000000 or 0.001+100000
  val gama = 0.9 //状态衰减率
  val max_learn = 1000000 //学习次数
  val lambda = 0.9
  //基因序列
  private var bestOne: Array[Int] = _
  //初始化染色体
  private var globalCities: Array[CityTMA] = _

  /**
    * 设置基因的信息
    * 如果信息太大就使用数据库作为替代
    *
    * @param cities 基因的信息
    */
  def setCities(cities: Array[CityTMA]): Unit = {
    globalCities = cities
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

    if (state == -1)
      return -1

    //1.当状态对应的行为的概率
    val state_actions = q_table(state)
    var action_name = -1
    //2. 选随机数 > epsilon 或者 处于初始化状态，随机选取行为
    if (math.random > epsilon || state_actions.forall(_ == 0.0) || state_actions.forall(_.isNaN)) {
      val choicef = action.diff(bestOne ++ Array(state))
      if (choicef.length == 0)
        action_name = state
      else {
        val choice = new Random().nextInt(choicef.length)
        action_name = choicef(choice)
      }
    } else {
      //3. 非2的情况下，选取概率最大的行为
      val sorted = state_actions.clone().zipWithIndex.sortBy(_._1)
      val temp = sorted.map(_._2).diff(bestOne ++ Array(state))
      if (temp.nonEmpty)
        action_name = sorted.filter(t => t._2 == temp.head).head._2
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
      if (!bestOne.contains(S))
        bestOne(bestOne.indexOf(-1)) = S
    }
    if (!bestOne.contains(-1)) {
      if (bestOne.distinct.length == bestOne.length) {
        S_ = -1
        val cost = cityCost.cityCost(bestOne)
        R = cost._1
        //println(cost)
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
  def update_env(q_table: Array[Array[Double]], episode: Int, step_counter: Int): Unit = {
    val bestOne = q_table.clone().map(s => {
      s.indexOf(s.max)
    })
    println(s"Episode ${episode + 1}: total_steps = $step_counter,best one:${bestOne.mkString(",")}")
    println(bestOne.distinct.length, bestOne.length)
    if (bestOne.distinct.length == bestOne.length) {
      println("+++++++++++++++++++++++++++++++++++++++++++++++")
      println("learned best one!!!")
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
    val eligibility_trace = q_table.clone()
    //2. repeat
    for (epsilon <- 0 to max_learn) {
      var step_counter = 0
      val cityCost = new CityCost()
      cityCost.setAll(aircraftMap.clone(), airportMap.clone(), cities.clone())
      resetBestOne()
      var S = new Random().nextInt(action.length)
      var A = choose_action(S, q_table)
      var is_terminated = false

      for (i <- eligibility_trace.indices; j <- eligibility_trace(i).indices) {
        eligibility_trace(i)(j) = 0.0
      }

      //update_env(S, epsilon, step_counter)
      while (!is_terminated) {
        //3. loop:当前状态选择行为
        //4. loop:当前状态和选择行为在环境中的反馈
        val next_states = get_env_feedback(cityCost, S, A)

        val A_next = choose_action(next_states._1, q_table)
        //5.loop: 理论采取当前状态采取当前的行为的反馈q值
        val q_predict = q_table(S)(action.indexOf(A))
        //6. loop:实际上反馈的q值
        var q_target = 0.0
        if (next_states._1 != -1)
        //下一次的状态和action
          q_target = next_states._2 + gama * q_table(next_states._1)(A_next)
        else {
          q_target = next_states._2
        }

        val error = q_target - q_predict

        eligibility_trace(S)(action.indexOf(A)) += 1

        //7. loop:更新q-table
        for (i <- q_table.indices; j <- q_table(i).indices) {
          q_table(i)(j) += learning_rate * error * eligibility_trace(i)(j)
          eligibility_trace(i)(j) *= gama * lambda
        }

        S = next_states._1
        A = A_next
        //update_env(S, epsilon, step_counter + 1)
        step_counter += 1

        if (S == -1) {
          is_terminated = true
        }
      }
      update_env(q_table, epsilon, step_counter)
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
      val longitudeDegr = row.getAs[Int]("LatitudeDegr")
      val longitudeMin = row.getAs[Int]("LatitudeMin")
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
