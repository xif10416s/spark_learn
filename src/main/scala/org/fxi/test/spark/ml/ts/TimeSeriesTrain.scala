package org.fxi.test.spark.ml.ts

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeries, TimeSeriesRDD}
import com.cloudera.sparkts.TimeSeries.timeSeriesFromIrregularSamples
import com.cloudera.sparkts.parsers.YahooParser
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

object TimeSeriesTrain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      //      .config("spark.jars", "/Users/seki/git/learn/bigData/scalaProject/scala-test-spark2.2/target/scala-test-spark2.2-1.0-SNAPSHOT.jar")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    var df = spark.read.option("header",true).csv("data/GOOG.csv")
    df.cache()
//    df.show()
//    val ts = YahooParser.yahooStringToTimeSeries(text, zone = ZoneId.of("Z"))

    import org.apache.spark.sql.functions._
    val zone = ZoneId.of("Z")
//    val code:(String) => ZonedDateTime = (d:String) => LocalDate.parse(d).atStartOfDay(zone);
//    val addCol = udf(code)
//    df = df.withColumn("Date",addCol(col("Date")))
//    df.show()


//    val times = df.select("Date").rdd.map(row => {
//      val str = row.getAs[String]("Date")
//      LocalDate.parse(str).atStartOfDay(zone)
//    }).collect()
//    times.foreach(println _)

    val startTime:String = "20140327"
    val endTime= "20140425"
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(startTime.substring(0, 4).toInt, startTime.substring(4, 6).toInt, startTime.substring(6).toInt, 0, 0, 0, 0, zone),
      ZonedDateTime.of(endTime.substring(0, 4).toInt, endTime.substring(4, 6).toInt, endTime.substring(6).toInt, 0, 0, 0, 0, zone),
      new DayFrequency(1))

    import java.time.format.DateTimeFormatter

    df = df.flatMap(row =>{
      val date = LocalDate.parse(row.getAs[String]("Date"))
      val dt = ZonedDateTime.of(date.getYear,date.getMonthValue,date.getDayOfMonth,0,0,0,0,zone)
      val tp =  Timestamp.from(dt.toInstant)
      val lst = ListBuffer[(Timestamp,String,Double)]()
      val open = (tp,"Open", row.getAs[String]("Open").toDouble)
      lst+= open
      val High = (tp,"High",row.getAs[String]("High").toDouble)
      lst+= High
      lst
    }).toDF("date","key","value").orderBy("date")
    df.show()


    //创建训练数据TimeSeriesRDD(key,DenseVector(series))
    val trainTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, df,
      "date", "key", "value")

    val vectors = trainTsrdd.values.collect()
    vectors.foreach(println _)

    //填充缺失值
    val filledTrainTsrdd = trainTsrdd.fill("linear")
    trainTsrdd.keys.foreach(println _)

    val predictedN = 3
    val model = new TimeSeriesModel(predictedN,"forecast")

    //只有holtWinters才有的参数
    //季节性参数（12或者4）
    val period=12
    //holtWinters选择模型：additive（加法模型）、Multiplicative（乘法模型）
    val holtWintersModelType="Multiplicative"

    val (forecast2, sse) = model.holtWintersModelTrainKey(filledTrainTsrdd, period, holtWintersModelType)
    //HoltWinters模型评估参数的保存
    model.holtWintersModelKeyEvaluationSave(spark, sse, forecast2)




    //创建和训练arima模型
    val (forecast, coefficients) = model.arimaModelTrainKey(filledTrainTsrdd, List[String]())
    //Arima模型评估参数的保存
    model.arimaModelKeyEvaluationSave(spark, coefficients, forecast)

    val sdf = new SimpleDateFormat(model.getDateType(startTime))
    /**
      * 7、合并实际值和预测值，并加上日期,形成dataframe(Date,Data)，并保存
      */
    model.actualForcastDateKeySaveInHive(spark, filledTrainTsrdd, forecast2, predictedN, startTime,
      endTime, "day", 1,sdf)
  }

}
