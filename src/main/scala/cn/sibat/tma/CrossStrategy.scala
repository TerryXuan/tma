package cn.sibat.tma

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 遗传算法的交叉策略
  */
object CrossStrategy {
  /**
    * 交叉策略: 单刀二代遗传
    * 一刀切策略，基因切一半交换
    * 个体a，切完变成a1,a2
    * 个体b，切完变成b1,b2
    * 组合规则，a1-b1-a2-b2
    * 前半部分包含的话会被去掉，即保持基因完整
    * 二代遗传
    *
    * @param route1 染色体1
    * @param route2 染色体2
    * @return (child1,child2)
    */
  def cross(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = new ArrayBuffer[Int]()
    val child2 = new ArrayBuffer[Int]()
    val indexCount = route1.length
    val a1 = route1.slice(0, indexCount / 2)
    val b1 = route2.slice(0, indexCount / 2)
    val a2 = route1.slice(indexCount / 2 + 1, indexCount)
    val b2 = route2.slice(indexCount / 2 + 1, indexCount)
    child1 ++= a1
    child1 ++= b1.diff(child1)
    child1 ++= a2.diff(child1)
    child1 ++= b2.diff(child1)
    child2 ++= b1
    child2 ++= a1.diff(child2)
    child2 ++= b2.diff(child2)
    child2 ++= a2.diff(child2)
    (child1.toArray, child2.toArray)
  }

  /**
    * 交叉策略:多刀二代遗传
    * 多刀切策略，基因切多刀切，多刀数随机从区间1到sqrt(基因长度)选
    * 交叉操作，个体a，切完变成，a1,a2,a3,a4,...
    * 个体b,切完变成，b1,b2,b3,b4,...
    * 组合规则，a1-b1-a2-b2-a3-b3-...
    * 前半部分包含的话会被去掉，即保持基因完整
    * 二代遗传
    */
  def cross1(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = new ArrayBuffer[Int]()
    val child2 = new ArrayBuffer[Int]()
    val ran = new Random()
    val splitNum = ran.nextInt(math.sqrt(route1.length).toInt) + 1 //刀数
    val splitIndex = new Array[Int](splitNum) //切点
    for (i <- splitIndex.indices) {
      splitIndex(i) = ran.nextInt(route1.length)
    }
    val indexCount = splitIndex.distinct.sorted
    for (i <- 0 to splitIndex.length) {
      var ai = Array[Int]()
      var bi = Array[Int]()
      if (i == 0 && indexCount(0) != 0) { //首切点不为0，即基因片段为0到首切点
        ai = route1.slice(0, indexCount(i))
        bi = route2.slice(0, indexCount(i))
      } else if (i == indexCount.length && indexCount.last != route1.length) { //尾切掉不为总长，即最后的基因片段为尾切点到总长
        ai = route1.slice(indexCount.last + 1, route1.length)
        bi = route2.slice(indexCount.last + 1, route1.length)
      }
      child2 ++= bi.diff(child2)
      child2 ++= ai.diff(child2)
      child1 ++= ai.diff(child1)
      child1 ++= bi.diff(child1)
      if (i < indexCount.length - 1) {
        ai = route1.slice(indexCount(i) + 1, indexCount(i + 1))
        bi = route2.slice(indexCount(i) + 1, indexCount(i + 1))
      }
      //把基因片段串起来
      child2 ++= bi.diff(child2)
      child2 ++= ai.diff(child2)

      child1 ++= ai.diff(child1)
      child1 ++= bi.diff(child1)
    }

    (child1.toArray, child2.toArray)
  }

  /**
    * 交叉策略:PMX，二刀二代
    * 二刀切策略，基因切二刀切
    * 交叉操作，个体a，切完变成，a1,a2,a3
    * 个体b,切完变成，b1,b2,b3
    * 组合规则，A-b2-B，A-a1-B
    * 同数指向，具体见博客https://blog.csdn.net/u012750702/article/details/54563515
    * 二代遗传
    */
  def cross2(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = new Array[Int](route1.length)
    val child2 = new Array[Int](route2.length)
    val ran = new Random()
    val splitNum = 2 //刀数
    val splitIndex = new Array[Int](splitNum) //切点
    for (i <- splitIndex.indices) {
      splitIndex(i) = ran.nextInt(route1.length)
    }

    //交叉映射表
    val map = new mutable.HashMap[Int, Int]()

    //中间片保留
    for (i <- splitIndex.min to splitIndex.max) {
      child1(i) = route2(i)
      child2(i) = route1(i)
      map += ((route1(i), route2(i)))
    }
    //起点段A的更新
    for (i <- 0 until splitIndex.min) {
      var value1 = route1(i)
      var value2 = route2(i)
      while (map.exists(t => t._2 == value1)) {
        value1 = map.find(t => t._2 == value1).get._1
      }
      while (map.exists(t => t._1 == value2)) {
        value2 = map.find(t => t._1 == value2).get._2
      }
      child1(i) = value1
      child2(i) = value2
    }

    //尾段B的更新
    for (i <- splitIndex.max + 1 until route1.length) {
      var value1 = route1(i)
      var value2 = route2(i)
      while (map.exists(t => t._2 == value1)) {
        value1 = map.find(t => t._2 == value1).get._1
      }
      while (map.exists(t => t._1 == value2)) {
        value2 = map.find(t => t._1 == value2).get._2
      }
      child1(i) = value1
      child2(i) = value2
    }

    (child1, child2)
  }

  /**
    * 交叉策略:OX，二刀二代
    * 二刀切策略，基因切二刀切
    * 交叉操作，个体a，切完变成，a1,a2,a3
    * 个体b,切完变成，b1,b2,b3
    * 组合规则，A-b2-B，A-a1-B
    * 同数指向，具体见博客https://blog.csdn.net/u012750702/article/details/54563515
    * 二代遗传
    */
  def cross3(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = new Array[Int](route1.length)
    val child2 = new Array[Int](route2.length)
    val ran = new Random()
    val splitNum = 2 //刀数
    val splitIndex = new Array[Int](splitNum) //切点
    for (i <- splitIndex.indices) {
      splitIndex(i) = ran.nextInt(route1.length)
    }
    //中间片保留
    for (i <- splitIndex.min to splitIndex.max) {
      child1(i) = route1(i)
      child2(i) = route2(i)
    }
    //起点段A的更新
    var diff1 = route2.diff(child1)
    var diff2 = route1.diff(child2)
    for (i <- 0 until splitIndex.min) {
      child1(i) = diff1(i)
      child2(i) = diff2(i)
    }

    //尾段B的更新
    diff1 = route2.diff(child1)
    diff2 = route1.diff(child2)
    var count = 0
    for (i <- splitIndex.max + 1 until route1.length) {
      child1(i) = diff1(count)
      child2(i) = diff2(count)
      count += 1
    }

    (child1, child2)
  }

  /**
    * 交叉策略:PBX，多刀二代
    * 多刀切策略，基因切多刀切，多刀数随机从区间0到基因长度-基因长度的/5选
    * 交叉操作，个体a，切完变成，a1,a2,a3,...
    * 个体b,切完变成，b1,b2,b3,...
    * 组合规则，A-b2-B，A-a1-B
    * 同数指向，具体见博客https://blog.csdn.net/u012750702/article/details/54563515
    * 二代遗传
    */
  def cross4(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = route1.clone()
    val child2 = route2.clone()
    val ran = new Random()
    val splitNum = math.max(1, ran.nextInt(route1.length - route1.length / 5)) //刀数
    val shuffle1 = shuffle(route1)
    val shuffle2 = shuffle(route2)
    val stand1 = shuffle1.take(splitNum)
    val stand2 = shuffle2.take(splitNum)
    val indexs1 = new Array[Int](stand1.length)
    val indexs2 = new Array[Int](stand2.length)
    //记住parent2的选中基因的位置
    for (i <- stand1.indices) {
      indexs1(i) = route2.indexOf(stand1(i))
      indexs2(i) = route1.indexOf(stand2(i))
    }
    val sort1 = indexs1.sorted
    val sort2 = indexs2.sorted
    for (i <- sort1.indices) {
      child2(sort1(i)) = stand1(i)
      child1(sort2(i)) = stand2(i)
    }

    (child1, child2)
  }

  /**
    * 交叉策略:OBX，多刀二代
    * 多刀切策略，基因切多刀切，多刀数随机从区间0到基因长度-基因长度的/5选
    * 交叉操作，个体a，切完变成，a1,a2,a3,...
    * 个体b,切完变成，b1,b2,b3,...
    * 组合规则，A-b2-B，A-a1-B
    * 同数指向，具体见博客https://blog.csdn.net/u012750702/article/details/54563515
    * 二代遗传
    */
  def cross5(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = route1.clone()
    val child2 = route2.clone()
    val ran = new Random()
    val splitNum = math.max(1, ran.nextInt(route1.length - route1.length / 5)) //刀数
    val shuffle1 = shuffle(route1)
    val shuffle2 = shuffle(route2)
    val stand1 = shuffle1.take(splitNum)
    val stand2 = shuffle2.take(splitNum)
    val indexs1 = new Array[Int](stand1.length)
    val indexs2 = new Array[Int](stand2.length)
    //记住parent2的选中基因的位置
    for (i <- stand1.indices) {
      indexs1(i) = route2.indexOf(stand1(i))
      indexs2(i) = route1.indexOf(stand2(i))
    }
    val sort1 = indexs1.sorted
    val sort2 = indexs2.sorted
    for (i <- sort1.indices) {
      child2(sort1(i)) = stand1(i)
      child1(sort2(i)) = stand2(i)
    }

    (child1, child2)
  }

  /**
    * 交叉策略:CX，二代
    * 多刀切策略，基因切多刀切，多刀数随机从区间0到基因长度-基因长度的/5选
    * 交叉操作，个体a，切完变成，a1,a2,a3,...
    * 个体b,切完变成，b1,b2,b3,...
    * 组合规则，A-b2-B，A-a1-B
    * 同数指向，具体见博客https://blog.csdn.net/u012750702/article/details/54563515
    * 二代遗传
    */
  def cross6(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    var child1 = route1
    var child2 = route2
    val start1 = route1.head
    val start2 = route2.head
    val cycle1 = new ArrayBuffer[Int]()
    val cycle2 = new ArrayBuffer[Int]()
    cycle1 += -1
    cycle2 += -1
    var temp1 = start1
    var temp2 = start2
    while (cycle1.last != start1) {
      val value = route2(route1.indexOf(temp1))
      temp1 = value
      cycle1 += value
    }

    while (cycle2.last != start2) {
      val value = route1(route2.indexOf(temp2))
      temp2 = value
      cycle2 += value
    }

    child1 = child1.map(i => if (cycle1.contains(i)) i else -1)
    child2 = child2.map(i => if (cycle2.contains(i)) i else -1)

    while (child1.indexOf(-1) > 0) {
      val index = child1.indexOf(-1)
      child1(index) = route2(index)
    }

    while (child2.indexOf(-1) > 0) {
      val index = child2.indexOf(-1)
      child2(index) = route1(index)
    }

    (child1, child2)
  }

  /**
    * 交叉策略:SEC，二代
    * 二刀切策略，基因切二刀切
    * 交叉操作，个体a，切完变成，a1,a2,a3
    * 个体b,切完变成，b1,b2,b3
    * 组合规则，A-b2-B，A-a1-B
    * 同数指向，具体见博客https://blog.csdn.net/u012750702/article/details/54563515
    * 二代遗传
    */
  def cross7(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val child1 = route1.clone()
    val child2 = route2.clone()
    val ran = new Random()
    val splitNum = 2 //刀数
    val splitIndex = new Array[Int](splitNum) //切点
    for (i <- splitIndex.indices) {
      splitIndex(i) = ran.nextInt(route1.length)
    }

    val exchange = route1.slice(splitIndex.min, splitIndex.max)
    val indexs = new Array[Int](exchange.length)

    for (i <- exchange.indices) {
      indexs(i) = route2.indexOf(exchange(i))
    }
    val indexsSort = indexs.sorted

    for (i <- indexs.indices) {
      child2(indexsSort(i)) = exchange(i)
      child1(route1.indexOf(exchange(i))) = route2(indexsSort(i))
    }
    (child1, child2)
  }

  /**
    * 随机选取交叉方法
    *
    * @param route1 染色体1
    * @param route2 染色体2
    * @return
    */
  def randomCross(route1: Array[Int], route2: Array[Int]): (Array[Int], Array[Int]) = {
    val ran = new Random().nextInt(8)
    ran match {
      case 0 => cross(route1, route2)
      case 1 => cross1(route1, route2)
      case 2 => cross2(route1, route2)
      case 3 => cross3(route1, route2)
      case 4 => cross4(route1, route2)
      case 5 => cross5(route1, route2)
      case 6 => cross6(route1, route2)
      case 7 => cross7(route1, route2)
    }
  }

  /**
    * 随机打乱数组，只打乱下标集合
    * 不改变原数组
    *
    * @param array 下标集合
    * @return
    */
  def shuffle(array: Array[Int]): Array[Int] = {
    val result = array.clone()
    var m = result.length
    var t = 0
    var i = 0
    while (m > 0) {
      i = (math.random * m).toInt
      m -= 1
      t = result(m)
      result(m) = result(i)
      result(i) = t
    }
    result
  }
}
