package cn.sibat.tma

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable

case class Test(id: Int, num: Array[Int])

object tmaUDF {
  def main(args: Array[String]): Unit = {
    val map = new mutable.HashMap[String, Test]()
    for (i <- 0 until 10) {
      map += ((i.toString, Test(i, (0 to 10).toArray)))
    }

    val existAircraft = map.filter(t => t._2.num.contains(2))
    if (existAircraft.nonEmpty) {
      val a = existAircraft.head._2
      val cityTMA = a.num.find(c => c == 2).get
      a.num.update(a.num.indexOf(cityTMA), 12)
    }

    map.foreach(t=>{
      println(t._1,t._2.id,t._2.num.mkString(";"))
    })
  }
}
