package org.fix.test.jyyb

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OracleToMysql {
  val spark = SparkSession
    .builder
    .appName("Spark Pi")
    .master("local[*]")
    .getOrCreate()

  val dbUrl = "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb"
  val dbUser = "shengji"
  val dbPass = "Shengji#2020"

  def main(args: Array[String]): Unit = {
    for( a <- 0 until 9){
      println( "Value of a: " + a % 9 );
    }
  }

  def testJdbcSave():Unit = {

    val jdbcDF = spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("query","select * from test").option("user", "root").option("password", "12345678").load()
 //repartition(72,col("BAZ001"))
    jdbcDF.write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", "kc_base").option("user", "root").option("password", "12345678").save()

    jdbcDF.join(jdbcDF,jdbcDF.col("").equalTo(jdbcDF.col("").and(jdbcDF.col("").equalTo(jdbcDF.col("")))))
  }

  def doSummary(dept: String): Unit = {
    val partition = "partition(P201910)"
    //    val jdbcDF = spark.read.format("jdbc").option("url", dbUrl).option("query", s"select * from JYSI.KC22 ${partition} where 1=1 and  AKB020 = 1001 and mod(BAZ001/10  , 10) = 0  ").option("user", dbUser).option("password", dbPass).load().cache()

    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query", s"select ${selectCols}  from JYSI.KC22 ${partition} where AKB020 = 1001 and mod(BAZ001/10  , 10) = 0  ").option("user", "shengji").option("password", "Shengji#2020").load().cache()

    import java.math.BigDecimal

    import org.apache.spark.ml.linalg.{Vector, Vectors}
    import org.apache.spark.ml.stat.Summarizer._
    import org.apache.spark.sql.functions.{col, udf}
    import spark.implicits._

    val toVec = udf((BAZ001: Long, AAE019: BigDecimal, AKC228: BigDecimal) => Vectors.dense(BAZ001, AAE019.doubleValue(), AKC228.doubleValue()))

    val statDF = jdbcDF.withColumn("statVec", toVec(col("BAZ001"), col("AAE019"), col("AKC228")))

    val (meanVal2, varianceVal2, count2) = statDF.select(mean($"features"), variance($"features"), max($"features"))
      .as[(Vector, Vector, Vector)].first()

    jdbcDF.select("AKC221")

    jdbcDF.describe().coalesce(1).write.format("csv").option("header", "true").option("charset", "gbk").save("/home/spark/result/p201901_desc")

    jdbcDF.coalesce(1).write.format("csv").option("header", "true").option("charset", "gbk").save("/home/data/result/p201901")

    jdbcDF.groupBy("AKF002").sum("AAE019").orderBy($"sum(AAE019)".desc)
  }


  def loadYearData(year: Int) : DataFrame = {
    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    val p04DF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query", s"select ${selectCols}  from JYSI.KC22 partition(P${year}04) where AKB020 = 1001   ").option("user", "shengji").option("password", "Shengji#2020").load()
    val p07DF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query", s"select ${selectCols}  from JYSI.KC22 partition(P${year}07) where AKB020 = 1001   ").option("user", "shengji").option("password", "Shengji#2020").load()
    val p10DF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query", s"select ${selectCols}  from JYSI.KC22 partition(P${year}10) where AKB020 = 1001   ").option("user", "shengji").option("password", "Shengji#2020").load()
    var resultDF = p04DF.union(p07DF).union(p10DF)
    val lastPartition = year + 1
    val numPartition = 12 * 6 * 5
    for( i <- 0 until numPartition){
      val selectSql = s"select ${selectCols}  from JYSI.KC22 partition(P${lastPartition}01) where AKB020 = 1001 and mod(trunc(BAZ001/10)  , ${numPartition}) = ${i}    "
      println(selectSql)
      resultDF= resultDF.union(spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query",selectSql).option("user", "shengji").option("password", "Shengji#2020").load())
    }
    resultDF.repartition(numPartition,col("BAZ001"))
  }


  def loadYearData2017(partition: String) : DataFrame = {
    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    var resultDF = spark.emptyDataFrame
    val numPartition = 12 * 6
    for( i <- 0 until numPartition){
      val selectSql = s"select ${selectCols}  from JYSI.KC22 partition(P${partition}) where AKB020 = 1001 and mod(trunc(BAZ001/10)  , ${numPartition}) = ${i}    "
      println(selectSql)
      val loadDf = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query",selectSql).option("user", "shengji").option("password", "Shengji#2020").load()
      if(i == 0){
        resultDF = loadDf
      } else {
         resultDF= resultDF.union(loadDf)
      }
    }
    resultDF
  }


  //  .write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", "KC21").option("user", "root").option("password", "12345678").save()
  def loadNormal(table: String) : DataFrame = {
//    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    var resultDF = spark.emptyDataFrame
    val numPartition = 12 * 6
    for( i <- 0 until numPartition){
      val selectSql = s"select *  from JYSI.${table}  where AKB020 = 1001 and mod(trunc(BAZ001/10)  , ${numPartition}) = ${i}    "
      println(selectSql)
      val loadDf = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query",selectSql).option("user", "shengji").option("password", "Shengji#2020").load()
      if(i == 0){
        resultDF = loadDf
      } else {
        resultDF= resultDF.union(loadDf)
      }
    }
    resultDF
  }

  val tables = Array("KA46","KC22_41","KC62","KA11_1","KA24","KA41","KA53","KC54","KE30","KE33","STRB_AE80","STRB_AE81","TD01","KA02","KA02_WUXI","KA03","KA05","KA06","KA11","KA12","KA12_1","KA52","KAC1","KC22","KC63","KC64","KC67","KE32","KZ22","KZ24","TDK43","KB03","KC60","KC61","KC52","KA57","KA48","KR01","KF56","KZ60","KA44","KE09","KE11","KF53","KF54","KF82","KA45","KA47","KA49","KA40","KA50","KA51","KA83","KC21","KC47","KC51","KZ21","TDK42")
  def sampleCsv(tables: Array[String]) : Unit = {
    //    val selectCols = "BAZ001,AKC190,AAE072,AKC220,AKE001,AKE002,AKE005,AKE006,AAC001,AKC221,AKE010,AKA065, AKE003 , AKA063 , AKC225,AKC226 ,AAE019 , AKC228 , AKE051 ,AKA068,AKC268 , BKA635,AKA069 ,  BKA107 , BKA108, BKC125 ,BKA634 ,BKA631 ,BKA632 ,BKA636 ,BKA637 ,AAE011, BKF050,AKF002,BKA650,AKA130,AKA120,BKA135"
    for (table <- tables) {
      val loadDf = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@//192.168.253.2:1521/jyjb").option("query",s"select * from JYSI.${table} where rownum < 100 ").option("user", "shengji").option("password", "Shengji#2020").load()
      loadDf.coalesce(1).write.format("csv").option("header", "true").option("charset", "gbk").save(s"/home/viya003/share/jyyb_temp/${table}")
    }
  }


}
