package cn.sibat.tma

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NeedInfo {

  def fitness(): Double = {

    0.0
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
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
      if (ConnFlightCode.equals("MLE")) {
        "6:00 am-8:00 am"
      } else if (ConnFlightCode.equals("DMLE")) {
        "9:00 am-11:00 am"
      } else if (ConnFlightCode.equals("DMLED")) {
        "5:55:00AM-(5:55:00AM-30)"
      } else if (ConnFlightCode.equals("MLED")) {
        "3:45 pm-(6:11:00PM-30)"
      } else if (direction.equals("Arriving")) {
        "time+45min-time+165min"
      } else if (direction.equals("Departing")) {
        "(time-120min)-(time-180min)"
      }

      direction
    })

    val legDF = SLACustomerWise.join(actualPAXAllocation, $"Name" === $"BookedByCustomer").select("DEPAirport", "ARRAirport", "Requirementvalue")

    //1248
    val join = filter.join(flyTime, data.col("Direction") === flyTime.col("Direction") && data.col("ConnFlightCode") === flyTime.col("CFNumber"), "left_outer")
      .withColumn("timeWindow", timeWindow(col("ConnFlightCode"), col("TodaysSTA"), col("Direction"))).join(legDF, Seq("DepAirport", "ArrAirport"))

    //基因
    val gene = join.groupByKey(row => row.getAs[String]("BookingCode") + "," + row.getAs[String]("ConnFlightCode")).flatMapGroups((str, it) => {
      val arr = it.toArray
      val passengerWeight = arr.map(_.getAs[Double]("PassengerWeight")).sum
      val baggageWeight = arr.map(_.getAs[Double]("baggageWeight")).sum
      val row = arr.head
      val direction = row.getAs[String]("Direction")
      val oName = row.getAs[String]("DepAirport")
      val dName = row.getAs[String]("ArrAirport")
      val legs = row.getAs[Int]("Requirementvalue")
      val Array(startTime, endTime) = row.getAs[String]("timeWindow").split("-")
      val o = CityTMA(str, "o", direction, oName, legs, arr.length, passengerWeight, baggageWeight, startTime, endTime)
      val d = CityTMA(str, "d", direction, dName, legs, -arr.length, passengerWeight, baggageWeight, startTime, endTime)
      Array(o, d)
    })

    //染色体
    val chromosome = gene.rdd.zipWithIndex()

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
