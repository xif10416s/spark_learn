// ./bin/spark-shell -i <filename.scala --conf spark.driver.args=("arg1 arg2")
//  /home/spark/spark-2.4.4-bin-hadoop2.7/bin/spark-shell -i /home/spark/tasks/LoadKC22.scala --conf spark.driver.args=2018  --master local[*] --driver-memory 60g --executor-memory 60G  --jars  /home/spark/OJDBC-Full/ojdbc6.jar,/home/spark/mysql-connector-java-5.1.38.jar  --driver-class-path /home/spark/OJDBC-Full/ojdbc6.jar:/home/spark/mysql-connector-java-5.1.38.jar
/**
  * /home/spark/spark-2.4.4-bin-hadoop2.7/bin/spark-shell -i /home/spark/tasks/LoadKC22.scala --conf spark.driver.args=2018  --master local[*] --driver-memory 60g --executor-memory 60G  --jars  /home/spark/OJDBC-Full/ojdbc6.jar,/home/spark/mysql-connector-java-5.1.38.jar  --driver-class-path /home/spark/OJDBC-Full/ojdbc6.jar:/home/spark/mysql-connector-java-5.1.38.jar
  */

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val year = sc.getConf.get("spark.driver.args").toInt

println(year)

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

val df = loadYearData(year)

df.write.format("jdbc").mode("append").option("driver","com.mysql.jdbc.Driver").option("url", "jdbc:mysql://localhost:3306/jyyb").option("dbtable", s"KC22_${year}").option("user", "root").option("password", "12345678").save()

System.exit(0)