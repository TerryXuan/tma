package cn.sibat.tma

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * tma 需求提取和组成基因
  *
  * @author kong
  */
object NeedInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //VersionNo,BookingCode,BookingDestinationLineNo,BookingPassengerLineNo,PassengerNo,Date,DepAirport,ArrAirport,Direction,Earliestdeptime,Latestdeptime,Earliestarrtime,Latestarrtime,VIP,OrderType,TicketType,Standby,Proximityzone,Region,PassengerWeight,BaggageWeight,Locked,ConnFlightCode,Allocatedtoflight,FlightNo,LegNo,Name,Note,SpecialInstructions,PassengerType,Noofseats,Earliestdeptimecontract,Latestdeptimecontract,Earliestarrtimecontract,Latestarrtimecontract,InternalConflict,InternalConflictDescription,Latebooking,Minairtime,Conflictcanbeoverruled,ConflictOverruled,IncludepassengerinIndex,ReleaseConflict,ConnFlightNo,ReleaseConflictDescription,Sell-toCustomerNo,LegId,LateDeparture,LegId2,PaymentType,LegNo(Disembarging),VIPType
    val data = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/pub-Transporation.csv")
    //AircraftType,Airline,ConnAirport,Direction,CFNumber,ScheduleDay,TodaysSTA,NumberofPassenger,TodaysETA,AirlineInfo,TodaysATA,RelatedFlightNo
    val flyTime = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/INTLFLTForTheDay.csv")
    //Date,Sunrise,Sunset,Tides,Lowtide,Hightide2,GroundingTime,TWILFrom,TWILTo
    val daylightTime = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/DayLightTime.csv")
    //Code,Discount,Weight,Stdbaggageofmaxweight,DefaultPaymentType,Takeupseat,Minage,Maxage,DisableCheckinBookingAppr
    val passengerType = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/PassengerType.csv")
    //Date,AirCraft,Minutes
    val availableAircraftTime = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/AvailableAircraftTime.csv")
    //No,GTOW,SeatCapacity,APS
    val availablePayload = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/AvailablePayload.csv")
    //temp,A,lon,lat,B,lon,lat,dis,time
    val dura_dist = spark.read.csv("E:/data/TMA/dura_dist")
    //No,Name,OperationalRequirementType,Dependency,MinMax,Requirementvalue
    val SLACustomerWise = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/SLACustomerWise.csv")
      .filter(trim($"OperationalRequirementType") === "Flight Legs").distinct()
    //Code,Name,MinDownTime,LatitudeDegr,LatitudeMin,Lat1100min,LongitudeDegr,LongitudeMin,Long1100min,HeadingMale,DistanceMale,Latitude,Longitude,Proximityzone,FuelAvailable,Extradowntimeforfueling,Minintervalbetweenarrivals,South,West,PlanningArea,Phone,MgtType,Numberofbeds,Wheelbased,AirportCapacity
    val airportCordinates = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/AirportCordinates.csv")
    //num,Date,PAXNo,Name,Standby,NoShow,Terminal,Dock,Aircraft,GATE,Paymenttype,Ordertype,TicketType,PassengerType,Direction,Resort,BookedByCustomer,INTLFLT,CheckInfinishtime,FLTTimeDEPAirport,DEPAirport,FLTTimeARRAirport,ARRAirport,TMAFLT,VIPType,Captain,FirstOfficer,FlightAttendant,SpecialInstructions,Note
    val actualPAXAllocation = spark.read.option("header", value = true).csv("E:/data/TMA/dataset/H-ActualPAXAllocation.csv")

    val filter = data.filter(trim($"PassengerType").isin("CHILD", "FEMALE", "MALE") && trim($"TicketType").isin("DOCTORS", "HONEYMOONERS", "NORMAL FARE", "SPECIAL RATE", "SPEC. CONFM") && trim($"OrderType") === "TRANSFER" && trim($"ConnFlightCode") =!= "DEPTBA" && trim($"ConnFlightCode") =!= "ARRTBA" && trim($"Standby") === "No")

    val timeWindow = udf((ConnFlightCode: String, time: String, direction: String) => {
      //TodaysSTA,direction
      //arriving-> t < 3:15pm t+45min,t+165min
      //MLE-> 6:00 am-8:00 am
      //DMLE-> 9:00 am-11:00 am
      //departing-> t < 日落时间+1.5小时，t-120min,t-180min
      //DMLED-> 日出时间-日落时间前30分钟
      //MLED-> 15:45-日落时间前30分钟
      val sdf = new SimpleDateFormat("H:mm:ss")
      var value = "0-0"
      val sunsetArrival = "15:15:00"
      val sunrise = "5:55:00"
      val sunset = "18:11:00"
      val sunsetFly = sdf.format(sdf.parse(sunset).getTime - 30 * 60 * 1000)
      if (ConnFlightCode.equals("MLE")) {
        value = "6:00:00-8:00:00"
      } else if (ConnFlightCode.equals("DMLE")) {
        value = "9:00:00-11:00:00"
      } else if (ConnFlightCode.equals("DMLED")) {
        value = s"$sunrise-$sunsetFly"
      } else if (ConnFlightCode.equals("MLED")) {
        value = s"15:45:00-$sunsetFly"
      } else if (direction.equals("Arriving")) {
        val timeTrim = time.replaceAll(" ", "").toLowerCase
        var time24 = timeTrim
        if (timeTrim.contains("am")) {
          time24 = timeTrim.replace("am", "")
        } else if (timeTrim.contains("pm")) {
          val split = timeTrim.split(":")
          time24 = (split(0).toInt + 12) + ":" + split(1) + ":" + split(2)
        }
        val timestamp24 = sdf.parse(time24).getTime
        if (timestamp24 > sdf.parse(sunsetArrival).getTime)
          value = "6:00:00-8:00:00"
        else {
          value = s"${sdf.format(timestamp24 + 45 * 60 * 1000)}-${sdf.format(timestamp24 + 165 * 60 * 1000)}"
        }
      } else if (direction.equals("Departing")) {
        val timeTrim = time.replaceAll(" ", "").toLowerCase
        var time24 = timeTrim
        if (timeTrim.contains("am")) {
          time24 = timeTrim.replace("am", "")
        } else if (timeTrim.contains("pm")) {
          val split = timeTrim.split(":")
          time24 = (split(0).toInt + 12) + ":" + split(1) + ":" + split(2)
        }
        val timestamp24 = sdf.parse(time24).getTime
        if (timestamp24 > sdf.parse(sunset).getTime)
          value = s"15:45:00-$sunsetFly"
        else
          value = s"${sdf.format(timestamp24 - 180 * 60 * 1000)}-${sdf.format(timestamp24 - 120 * 60 * 1000)}"
      }
      value
    })

    val legDF = SLACustomerWise.join(actualPAXAllocation, $"Name" === $"BookedByCustomer").select("DepAirport", "ArrAirport", "Requirementvalue").distinct()
    val bLegs = spark.sparkContext.broadcast(legDF.collect())

    val withLegs = udf((depAirport: String, arrAirport: String) => {
      val legs = bLegs.value

      val targetAirport = if (depAirport.equals("MLE")) arrAirport else depAirport

      var value = "2"
      try {
        value = legs.filter(row => row.getAs[String]("DepAirport").equals(targetAirport) || row.getAs[String]("ArrAirport").equals(targetAirport)).head.getAs[String]("Requirementvalue")
      } catch {
        case e: Exception =>
      }
      value.toInt
    })

    //1248
    val join = filter.join(flyTime.select("CFNumber", "TodaysSTA"), data.col("ConnFlightCode") === flyTime.col("CFNumber"), "left_outer")
      .withColumn("timeWindow", timeWindow(col("ConnFlightCode"), col("TodaysSTA"), col("Direction")))
      .withColumn("legs", withLegs(col("DepAirport"), col("ArrAirport")))

    //基因445*2
    val gene = join.groupByKey(row => row.getAs[String]("BookingCode") + "," + row.getAs[String]("ConnFlightCode")).flatMapGroups((str, it) => {
      val arr = it.toArray
      val passengerWeight = arr.map(_.getAs[String]("PassengerWeight").toDouble).sum
      val baggageWeight = arr.map(_.getAs[String]("BaggageWeight").toDouble).sum
      val row = arr.head
      val direction = row.getAs[String]("Direction")
      val oName = row.getAs[String]("DepAirport")
      val dName = row.getAs[String]("ArrAirport")
      val legs = row.getAs[Int]("legs")
      val split = row.getAs[String]("timeWindow").split("-")
      val o = CityTMA(str, "o", direction, oName, legs, arr.length, passengerWeight, baggageWeight, split(0), split(1))
      val d = CityTMA(str, "d", direction, dName, legs, -arr.length, passengerWeight, baggageWeight, split(0), split(1))
      Array(o, d)
    })

    gene.repartition(1).write.mode("overwrite").option("header", value = true).csv("E:/data/TMA/dataset/gene")

    //println(gene.count())

    //染色体
    //val chromosome = gene.rdd.zipWithIndex()

    //闭环得分机制MLE->A->B->C->D->E->MLE
    //BookingCode 相同+ ConnFlightCode 相同 =》团体
    //DepAirport-ArrAirport=>task from dist_dura
    //飞机运行时间，日升到日落前半小时
    //飞机停留时间，机场25min、酒店15min，加油时间+5min
    //飞机耗油500lbs/h,最大油量2000lbs

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
