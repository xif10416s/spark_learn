package org.fxi.test.spark.ml.ts.test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeriesRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.fxi.test.spark.ml.ts.TimeSeriesModel

object SalesTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      //      .config("spark.jars", "/Users/seki/git/learn/bigData/scalaProject/scala-test-spark2.2/target/scala-test-spark2.2-1.0-SNAPSHOT.jar")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val shopDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.19.85.99:5432/retail-plus")
      .option("dbtable", "(select id,  mkt, gdid,to_char(date, 'YYYY-MM-DD') as  day ,xssl from shopdata ) as shop ")
      .option("user", "retail-plus")
      .option("password", "58IJ9tQ*L2kp*Og8*")
      .option("lowerBound", 24690001)
      .option("upperBound", 410178738)
      .option("numPartitions", 50)
      .option("partitionColumn", "id")
      .load()

    shopDF.cache()
    shopDF.show()

    val tDF = shopDF.filter("gdid = '3001063'")
    tDF.cache()
    import org.apache.spark.sql.functions.udf
    import spark.implicits._
    val mc = udf((d:String) => d.substring(0,7).replaceAll("-",""))
    var mDF=  tDF.withColumn("month",mc($"day"))
    val aggDF = mDF.groupBy("month","mkt","gdid").sum("xssl").withColumnRenamed("sum(xssl)","value").withColumn("value",$"value".cast(DoubleType)).orderBy("month")

    val startTime:String = "20160101"
    val endTime= "20180501"
    val predictedN = 3
    val model = new TimeSeriesModel(predictedN,"forecast")
    val sdf = new SimpleDateFormat(model.getDateType(startTime))

    val zone = ZoneId.systemDefault()


    val monthDay = udf((d:String) => {
      import java.time.format.DateTimeFormatter
      val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val day = d+"01"
      val date =  LocalDate.parse(day,formatter)
      val dt = ZonedDateTime.of(date.getYear,date.getMonthValue,date.getDayOfMonth,0,0,0,0,zone)
      val tp =  Timestamp.from(dt.toInstant)
      tp
    })

    val timeDF=aggDF.withColumn("time",monthDay($"month"))



    val dtIndex = model.getTimeSpan(startTime,endTime,"month",1,sdf)

    dtIndex.toZonedDateTimeArray().foreach(println _)

    //创建训练数据TimeSeriesRDD(key,DenseVector(series))
    val trainTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, timeDF, "time", "mkt", "value")

    //填充缺失值
    val filledTrainTsrdd = trainTsrdd.filter(f=>{
      var hasValue = false
      f._2.toArray.foreach(d =>{
        if(!d.isNaN){
          hasValue = true
        }
      })
      hasValue
    }).fill("nearest").fill("next")



    val (forecast, coefficients) = model.arimaModelTrainKey(filledTrainTsrdd, List[String]())
    //Arima模型评估参数的保存
    model.arimaModelKeyEvaluationSave(spark, coefficients, forecast)

    forecast.collect().foreach(println _)
  }
}
