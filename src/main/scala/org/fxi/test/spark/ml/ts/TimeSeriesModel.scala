package org.fxi.test.spark.ml.ts

import java.text.SimpleDateFormat
import java.time.{ZoneId, ZonedDateTime}
import java.util.Calendar

import com.cloudera.sparkts.models.{ARIMA, HoltWinters}
import com.cloudera.sparkts._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}

import scala.collection.mutable.ArrayBuffer

class TimeSeriesModel extends Serializable {
  //预测后面N个值
  private var predictedN = 1
  //存放的表名字
  private var outputTableName = "timeseries_output"

  def this(predictedN: Int, outputTableName: String) {
    this()
    this.predictedN = predictedN
    this.outputTableName = outputTableName
  }

  /**
    * 实现holtwinters模型，处理的数据多一个key列
    *
    * @param trainTsrdd
    * @param period
    * @param holtWintersModelType
    * @return
    */
  def holtWintersModelTrainKey(trainTsrdd: TimeSeriesRDD[String], period: Int, holtWintersModelType: String): (RDD[(String, Vector)], RDD[(String, Double)]) = {
    /** *参数设置 ******/
    //往后预测多少个值
    val predictedN = this.predictedN

    /** *创建HoltWinters模型 ***/
    //创建和训练HoltWinters模型.其RDD格式为(HoltWintersModel,Vector)
    val holtWintersAndVectorRdd = trainTsrdd.map { line =>
      line match {
        case (key, denseVector) =>
          (key, HoltWinters.fitModel(denseVector, period, holtWintersModelType), denseVector)
      }
    }

    /** *预测出后N个的值 *****/
    //构成N个预测值向量，之后导入到holtWinters的forcast方法中
    val predictedArrayBuffer = new ArrayBuffer[Double]()
    var i = 0
    while (i < predictedN) {
      predictedArrayBuffer += i
      i = i + 1
    }
    val predictedVectors = Vectors.dense(predictedArrayBuffer.toArray)

    //预测
    val forecast = holtWintersAndVectorRdd.map { row =>
      row match {
        case (key, holtWintersModel, denseVector) => {
          (key, holtWintersModel.forecast(denseVector, predictedVectors))
        }
      }
    }
    println("HoltWinters forecast of next " + predictedN + " observations:")
    forecast.foreach(println)

    /** holtWinters模型评估度量：SSE和方差 **/
    val sse = holtWintersAndVectorRdd.map { row =>
      row match {
        case (key, holtWintersModel, denseVector) => {
          (key, holtWintersModel.sse(denseVector))
        }
      }
    }
    return (forecast, sse)
  }

  /**
    * HoltWinters模型评估参数的保存
    * sse、mean、variance、standard_deviation、max、min、range、count
    *
    * @param sparkSession
    * @param sse
    * @param forecastValue
    */
  def holtWintersModelKeyEvaluationSave(sparkSession: SparkSession, sse: RDD[(String, Double)], forecastValue: RDD[(String, Vector)]): Unit = {
    /** 把vector转置 **/
    val forecastRdd = forecastValue.map {
      _ match {
        case (key, forecast) => forecast.toArray
      }
    }
    // Split the matrix into one number per line.
    val byColumnAndRow = forecastRdd.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    val summary = Statistics.colStats(transposed.map(value => Vectors.dense(value(0))))

    /** 统计求出预测值的均值、方差、标准差、最大值、最小值、极差、数量等;合并模型评估数据+统计值 **/
    //评估模型的参数+预测出来数据的统计值
    val evaluation = sse.join(forecastValue.map {
      _ match {
        case (key, forecast) => {
          (key, (summary.mean.toArray(0).toString,
            summary.variance.toArray(0).toString,
            math.sqrt(summary.variance.toArray(0)).toString,
            summary.max.toArray(0).toString,
            summary.min.toArray(0).toString,
            (summary.max.toArray(0) - summary.min.toArray(0)).toString,
            summary.count.toString))
        }
      }
    })

    val evaluationRddRow = evaluation.map {
      _ match {
        case (key, (sse, (mean, variance, standardDeviation, max, min, range, count))) => {
          Row(sse.toString, mean, variance, standardDeviation, max, min, range, count)
        }
      }
    }
    //形成评估dataframe
    val schemaString = "sse,mean,variance,standardDeviation,max,min,range,count"
    val schema = StructType(schemaString.split(",").map(fileName => StructField(fileName, StringType, true)))
    val evaluationDf = sparkSession.createDataFrame(evaluationRddRow, schema)

    println("Evaluation in HoltWinters:")
    evaluationDf.show()

  }

  /**
    * 实现Arima模型，处理数据是多一个key列
    *
    * @param trainTsrdd
    * @return
    */
  def arimaModelTrainKey(trainTsrdd: TimeSeriesRDD[String], listPDQ: List[String]): (RDD[(String, Vector)], RDD[(String, (String, (String, String, String), String, String))]) = {
    /** *参数设置 ******/
    val predictedN = this.predictedN

    /** *创建arima模型 ***/
    //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
    val arimaAndVectorRdd = trainTsrdd.map { line =>
      line match {
        case (key, denseVector) => {
          if (listPDQ.size >= 3) {
            (key, ARIMA.fitModel(listPDQ(0).toInt, listPDQ(1).toInt, listPDQ(2).toInt, denseVector), denseVector)
          } else {
            (key, ARIMA.autoFit(denseVector), denseVector)
          }
        }
      }
    }

    /** 参数输出:p,d,q的实际值和其系数值、最大似然估计值、aic值 **/
    val coefficients = arimaAndVectorRdd.map { line =>
      line match {
        case (key, arimaModel, denseVector) => {
          (key, (arimaModel.coefficients.mkString(","),
            (arimaModel.p.toString,
              arimaModel.d.toString,
              arimaModel.q.toString),
            arimaModel.logLikelihoodCSS(denseVector).toString,
            arimaModel.approxAIC(denseVector).toString))
        }
      }
    }

    coefficients.collect().map {
      _ match {
        case (key, (coefficients, (p, d, q), logLikelihood, aic)) =>
          println(key + " coefficients:" + coefficients + "=>" + "(p=" + p + ",d=" + d + ",q=" + q + ")")
      }
    }

    /** *预测出后N个的值 *****/
    val forecast = arimaAndVectorRdd.map { row =>
      row match {
        case (key, arimaModel, denseVector) => {
          (key, arimaModel.forecast(denseVector, predictedN))
        }
      }
    }

    //取出预测值
    val forecastValue = forecast.map {
      _ match {
        case (key, value) => {
          val partArray = value.toArray.mkString(",").split(",")
          var forecastArrayBuffer = new ArrayBuffer[Double]()
          var i = partArray.length - predictedN
          while (i < partArray.length) {
            forecastArrayBuffer += partArray(i).toDouble
            i = i + 1
          }
          (key, Vectors.dense(forecastArrayBuffer.toArray))
        }
      }
    }

    println("Arima forecast of next " + predictedN + " observations:")
    forecastValue.foreach(println)
    return (forecastValue, coefficients)
  }


  /**
    * Arima模型评估参数的保存
    * coefficients、（p、d、q）、logLikelihoodCSS、Aic、mean、variance、standard_deviation、max、min、range、count
    *
    * @param sparkSession
    * @param coefficients
    * @param forecastValue
    */
  def arimaModelKeyEvaluationSave(sparkSession: SparkSession, coefficients: RDD[(String, (String, (String, String, String), String, String))], forecastValue: RDD[(String, Vector)]): Unit = {
    /** 把vector转置 **/
    val forecastRdd = forecastValue.map {
      _ match {
        case (key, forecast) => forecast.toArray
      }
    }
    // Split the matrix into one number per line.
    val byColumnAndRow = forecastRdd.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    val summary = Statistics.colStats(transposed.map(value => Vectors.dense(value(0))))

    /** 统计求出预测值的均值、方差、标准差、最大值、最小值、极差、数量等;合并模型评估数据+统计值 **/
    //评估模型的参数+预测出来数据的统计值
    val evaluation = coefficients.join(forecastValue.map {
      _ match {
        case (key, forecast) => {
          (key, (summary.mean.toArray(0).toString,
            summary.variance.toArray(0).toString,
            math.sqrt(summary.variance.toArray(0)).toString,
            summary.max.toArray(0).toString,
            summary.min.toArray(0).toString,
            (summary.max.toArray(0) - summary.min.toArray(0)).toString,
            summary.count.toString))
        }
      }
    })

    val evaluationRddRow = evaluation.map {
      _ match {
        case (key, ((coefficients, pdq, logLikelihoodCSS, aic), (mean, variance, standardDeviation, max, min, range, count))) => {
          Row(coefficients, pdq.toString, logLikelihoodCSS, aic, mean, variance, standardDeviation, max, min, range, count)
        }
      }
    }

    //形成评估dataframe
    val schemaString = "coefficients,pdq,logLikelihoodCSS,aic,mean,variance,standardDeviation,max,min,range,count"
    val schema = types.StructType(schemaString.split(",").map(fileName => StructField(fileName, StringType, true)))
    val evaluationDf = sparkSession.createDataFrame(evaluationRddRow, schema)

    println("Evaluation in Arima:")
    evaluationDf.show()

  }


  /**
    * 合并实际值和预测值，并加上日期,形成dataframe(Date,Data)
    *
    * @param sparkSession
    * @param trainTsrdd
    * @param forecastValue
    * @param predictedN
    * @param startTime
    * @param endTime
    * @param timeSpanType
    * @param timeSpan
    * @param sdf
    * @param hiveColumnName
    */
  def actualForcastDateKeySaveInHive(sparkSession: SparkSession, trainTsrdd: TimeSeriesRDD[String], forecastValue: RDD[(String, Vector)],
                                     predictedN: Int, startTime: String, endTime: String, timeSpanType: String, timeSpan: Int,
                                     sdf: SimpleDateFormat): Unit = {
    //在真实值后面追加预测值
    val actualAndForcastRdd = trainTsrdd.map {
      _ match {
        case (key, actualValue) => (key, actualValue.toArray.mkString(","))
      }
    }.join(forecastValue.map {
      _ match {
        case (key, forecastValue) => (key, forecastValue.toArray.mkString(","))
      }
    })

    //获取从开始预测到预测后的时间，转成RDD形式
    val dateArray = productStartDatePredictDate(predictedN, timeSpanType, timeSpan, sdf, startTime, endTime)

    val dateRdd = sparkSession.sparkContext.parallelize(dateArray.toArray.mkString(",").split(",").map(date => (date)))

    //合并日期和数据值,形成RDD[Row]+keyName
    val actualAndForcastArray = actualAndForcastRdd.collect()
    for (i <- 0 until actualAndForcastArray.length) {
      val dateDataRdd = actualAndForcastArray(i) match {
        case (key, value) => {
          val actualAndForcast = sparkSession.sparkContext.parallelize(value.toString().split(",")
            .map(data => {
              data.replaceAll("\\(", "").replaceAll("\\)", "")
            }))
          println(actualAndForcast.count())
          println(dateRdd.count())
          println(dateRdd.first())
          dateRdd.zip(actualAndForcast).collect().foreach(println _)
        }
      }
      //保存信息
    }
  }

  /**
    * 批量生成日期，时间段为：训练数据的开始到预测的结束
    *
    * @param predictedN
    * @param timeSpanType
    * @param timeSpan
    * @param format
    * @param startTime
    * @param endTime
    * @return
    */
  def productStartDatePredictDate(predictedN: Int, timeSpanType: String, timeSpan: Int,
                                  format: SimpleDateFormat, startTime: String, endTime: String): ArrayBuffer[String] = {
    //形成开始start到预测predicted的日期
    val cal1 = Calendar.getInstance()
    cal1.setTime(format.parse(startTime))
    val cal2 = Calendar.getInstance()
    cal2.setTime(format.parse(endTime))

    /**
      * 获取时间差
      */
    var field = 1
    var diff: Long = 0
    timeSpanType match {
      case "year" => {
        field = Calendar.YEAR
        diff = (cal2.getTime.getYear() - cal1.getTime.getYear()) / timeSpan + predictedN;
      }
      case "month" => {
        field = Calendar.MONTH
        diff = ((cal2.getTime.getYear() - cal1.getTime.getYear()) * 12 + (cal2.getTime.getMonth() - cal1.getTime.getMonth())) / timeSpan + predictedN
      }
      case "day" => {
        field = Calendar.DATE
        diff = (cal2.getTimeInMillis - cal1.getTimeInMillis) / (1000 * 60 * 60 * 24) / timeSpan + predictedN
      }
      case "hour" => {
        field = Calendar.HOUR
        diff = (cal2.getTimeInMillis - cal1.getTimeInMillis) / (1000 * 60 * 60) / timeSpan + predictedN
      }
      case "minute" => {
        field = Calendar.MINUTE
        diff = (cal2.getTimeInMillis - cal1.getTimeInMillis) / (1000 * 60) / timeSpan + predictedN;
      }
    }

    var iDiff = 0L;
    var dateArrayBuffer = new ArrayBuffer[String]()
    while (iDiff <= diff) {
      //保存日期
      dateArrayBuffer += format.format(cal1.getTime)
      cal1.add(field, timeSpan)
      iDiff = iDiff + 1;
    }
    dateArrayBuffer
  }

  import java.util.regex.Pattern

  /**
    * 获取时间类型格式
    *
    * @param timeStr
    * @return
    */
  def getDateType(timeStr: String): String = {
    val dateRegFormat = new java.util.HashMap[String,String]()
    dateRegFormat.put("^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$", "yyyy-MM-dd HH:mm:ss") //2014年3月12日 13时5分34秒，2014-03-12 12:05:34，2014/3/12 12:5:34

    dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd HH:mm") //2014-03-12 12:05

    dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd HH") //2014-03-12 12

    dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd") //2014-03-12

    dateRegFormat.put("^\\d{4}\\D+\\d{2}$", "yyyy-MM") //2014-03

    dateRegFormat.put("^\\d{4}$", "yyyy") //2014

    dateRegFormat.put("^\\d{14}$", "yyyyMMddHHmmss") //20140312120534

    dateRegFormat.put("^\\d{12}$", "yyyyMMddHHmm") //201403121205

    dateRegFormat.put("^\\d{10}$", "yyyyMMddHH") //2014031212

    dateRegFormat.put("^\\d{8}$", "yyyyMMdd") //20140312

    dateRegFormat.put("^\\d{6}$", "yyyyMM") //201403

    try {
      import scala.collection.JavaConversions._
      for (key <- dateRegFormat.keySet) {
        if (Pattern.compile(key).matcher(timeStr).matches) {
          val formater = ""
          if (timeStr.contains("/")) return dateRegFormat.get(key).replaceAll("-", "/")
          else return dateRegFormat.get(key)
        }
      }
    } catch {
      case e: Exception =>
        System.err.println("-----------------日期格式无效:" + timeStr)
        e.printStackTrace()
    }
    null
  }


  import java.text.SimpleDateFormat

  def fromatData(time: String, format: SimpleDateFormat): String = {
    try {
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      return formatter.format(format.parse(time))
    } catch {
      case e: Exception =>
        e.printStackTrace
    }
    null
  }

  /**
    * 获取时间区间与时间跨度
    *
    * @param timeSpanType
    * @param timeSpan
    * @param sdf
    * @param startTime
    * @param endTime
    */
  def getTimeSpan(startTime: String, endTime: String, timeSpanType: String, timeSpan: Int, sdf: SimpleDateFormat): UniformDateTimeIndex = {
    val start = fromatData(startTime, sdf)
    val end = fromatData(endTime, sdf)

    val zone = ZoneId.systemDefault()
    val frequency = timeSpanType match {
      case "year" => new YearFrequency(timeSpan);
      case "month" => new MonthFrequency(timeSpan);
      case "day" => new DayFrequency(timeSpan);
      case "hour" => new HourFrequency(timeSpan);
      case "minute" => new MinuteFrequency(timeSpan);
    }

    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(start.substring(0, 4).toInt, start.substring(5, 7).toInt, start.substring(8, 10).toInt,
        start.substring(11, 13).toInt, start.substring(14, 16).toInt, 0, 0, zone),
      ZonedDateTime.of(end.substring(0, 4).toInt, end.substring(5, 7).toInt, end.substring(8, 10).toInt,
        end.substring(11, 13).toInt, end.substring(14, 16).toInt, 0, 0, zone),
      frequency)
    return dtIndex
  }
}
