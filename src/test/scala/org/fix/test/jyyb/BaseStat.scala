package org.fix.test.jyyb
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object BaseStat {
  val spark = SparkSession
    .builder
    .appName("Spark Pi")
    .master("local[*]")
    .getOrCreate()

  val jdbcDF = spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("query","select * from test").option("user", "root").option("password", "12345678").load()


  def loadYearData(table: String) : DataFrame = {
    //    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    var resultDF = spark.emptyDataFrame
    val numPartition = 12 * 10
    for( i <- 0 until numPartition){
      val selectSql = s"select *  from ${table} where mod(BAZ001 , ${numPartition}) = ${i}    "
      println(selectSql)
      val loadDf = spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("query",selectSql).option("user", "root").option("password", "12345678").load()
      if(i == 0){
        resultDF = loadDf
      } else {
        resultDF= resultDF.union(loadDf)
      }
    }
    resultDF
  }

  def loadKC22YearDataByTime(year: String , field: String) : DataFrame = {
    //    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    var resultDF = spark.emptyDataFrame
    val timeArray = Array((s"'$year-01-01 00:00:00'",s"'$year-02-01 00:00:00'"),
      (s"'$year-02-01 00:00:00'",s"'$year-03-01 00:00:00'"),
      (s"'$year-03-01 00:00:00'",s"'$year-04-01 00:00:00'"),
      (s"'$year-04-01 00:00:00'",s"'$year-05-01 00:00:00'"),
      (s"'$year-05-01 00:00:00'",s"'$year-06-01 00:00:00'"),
      (s"'$year-06-01 00:00:00'",s"'$year-07-01 00:00:00'"),
      (s"'$year-07-01 00:00:00'",s"'$year-08-01 00:00:00'"),
      (s"'$year-08-01 00:00:00'",s"'$year-09-01 00:00:00'"),
      (s"'$year-09-01 00:00:00'",s"'$year-10-01 00:00:00'"),
      (s"'$year-10-01 00:00:00'",s"'$year-11-01 00:00:00'"),
      (s"'$year-11-01 00:00:00'",s"'$year-12-01 00:00:00'"),
      (s"'$year-12-01 00:00:00'",s"'${year+1}-01-01 00:00:00'"))
    for (i <- 0 until timeArray.length){
      val time = timeArray(i)
      val selectSql = s"select *  from kc22_${year} where ${field} >= ${time._1} and ${field} < ${time._2}"
      println(selectSql)
      val loadDf = spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("query",selectSql).option("user", "root").option("password", "12345678").load()
      if(i == 0){
        resultDF = loadDf
      } else {
        resultDF= resultDF.union(loadDf)
      }
    }
    resultDF
  }

//  {
//    val tmpDf = dfkc21.filter("AKB020 = 1001").select("AKC190","AAC001").groupBy("AKC190","AAC001").count().filter("count > 1")
//    val tmpDf60 = dfkc21.filter("AKB020 = 1001")
//    tmpDf60.join(tmpDf,tmpDf.col("AKC190").equalTo(tmpDf60.col("AKC190")).and(tmpDf.col("AAC001").equalTo(tmpDf60.col("AAC001"))),"leftsemi").coalesce(1).write.format("csv").option("header", "true").option("charset", "gbk").save("/tmp/share/stat/kc21/重复")
//  }
//
//  {
//    val tmpDf = dfkc60.filter("AKB020 = 1001").select("AKC190","AAE072").groupBy("AKC190","AAE072").count().filter("count = 1")
//    val tmpDf60 = dfkc60.filter("AKB020 = 1001")
//    val merge60 =  tmpDf60.join(tmpDf,tmpDf.col("AKC190").equalTo(tmpDf60.col("AKC190")).and(tmpDf.col("AAE072").equalTo(tmpDf60.col("AAE072"))),"leftsemi")
//
//    val tmp21 = dfkc21.filter("AKB020 = 1001").select("AKC190","AAC001").groupBy("AKC190","AAC001").count().filter("count = 1")
//    val tmpDf21 = dfkc21.filter("AKB020 = 1001")
//    val merge21 = tmpDf21.join(tmp21,tmp21.col("AKC190").equalTo(tmpDf21.col("AKC190")).and(tmp21.col("AAC001").equalTo(tmpDf21.col("AAC001"))),"leftsemi").select("AKC190","AAC001","AAZ500","BKC192","BKC194","BKC232","AKC273","BKF050","AKF002","BKC317","AAE100","AKA130","BKC233","AKC196","AAE135")
//
//    val baseDF = merge60.join(merge21,merge60.col("AKC190").equalTo(merge21.col("AKC190")),"left")
//
//    baseDF.drop(merge21.col("AKC190")).write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", "kc_base_V2").option("user", "root").option("password", "12345678").save()
//
//  }
//  {
//    dfkcbase.filter("aka130=15").write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", "kc_base_15").option("user", "root").option("password", "12345678").save()
//
//    dfkcbase.filter("aka130=21").write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", "kc_base_21").option("user", "root").option("password", "12345678").save()
//
//    dfkcbase.filter("aka130=11 and akf002 in ('简易门诊','心血管内科','内分泌科')").write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", "kc_base_11_top3").option("user", "root").option("password", "12345678").save()
//
//  }

  {
    import spark.implicits._
    val targetUserDf = spark.sparkContext.textFile("file://user/data").toDF("userId")

    val userDateDf = jdbcDF.filter("bkc194 is not null").select(col("AAC001"), substring(col("bkc194").alias("bkc194"), 0, 10))
      .distinct().filter("bkc194 >= '2019-07-01' and  bkc194 <= '2019-12-31' ").join(targetUserDf, col("AAC001").equalTo(col("userId")), "leftsemi")

    userDateDf.cache()
    val dateList = userDateDf.select("bkc194").distinct().orderBy(col("bkc194")).collect().map(_.getAs[String]("bkc194"))

    val dateIndexMap = dateList.zipWithIndex.toMap

    val bdMap = spark.sparkContext.broadcast(dateIndexMap)


  }
}
