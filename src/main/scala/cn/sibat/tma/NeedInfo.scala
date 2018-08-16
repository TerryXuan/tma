package cn.sibat.tma

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NeedInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val data = spark.read.option("header", value = true).csv("E:/data/TMA/Datasheet for 02052018 ver 1.8.csv")
    val flyTime = spark.read.option("header", value = true).csv("E:/data/TMA/Datasheet1.8-flt.csv")

    val join = data.join(flyTime, data.col("Direction") === flyTime.col("Direction") && data.col("ConnFlightCode") === flyTime.col("CFNumber"), "outer")
    val count = join.filter($"OrderType" === "TRANSFER" && $"ConnFlightCode" =!= "DEPTBA" && $"ConnFlightCode" =!= "ARRTBA").count()
    println(count)

    val timeWindow = udf((ConnFlightCode: String, time: String, direction: String) => {
      //TodaysSTA,direction
      //arriving-> t < 3:15pm t+45min,t+165min
      //MLE-> 6:00 am-8:00 am
      //DMLE-> 9:00 am-11:00 am
      //departing-> t < 日落时间+1.5小时，t-120min,t-180min
      //DMLED-> 日出时间-日落时间前30分钟
      //MLED-> 15:45-日落时间前30分钟
      if (ConnFlightCode.equals("MLE")) {

      } else if (ConnFlightCode.equals("DMLE")) {

      } else if (ConnFlightCode.equals("DMLED")) {

      } else if (ConnFlightCode.equals("MLED")) {

      } else if (direction.equals("Arriving")) {

      } else if (direction.equals("Departing")) {

      }

      direction
    })

    //    data.createOrReplaceTempView("table_df")
    //    flyTime.createOrReplaceTempView("flyTime")
    //
    //    val in = "select * from table_df t,flyTime f where t.OrderType = 'TRANSFER' and t.Direction = f.Direction and t.ConnFlightCode = f.CFNumber"
    //    val transfer = "select * from table_df t where t.OrderType = 'TRANSFER'"
    //    val arrival = spark.sql(in)
    //    val tran = spark.sql(transfer)
    //    println(arrival.count())
    //    println(tran.count())
  }
}
