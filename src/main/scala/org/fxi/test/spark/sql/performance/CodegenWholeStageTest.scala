package org.fxi.test.spark.sql.performance

import org.apache.spark.sql.SparkSession

/**
  * Created by xifei on 16-8-25.
  */
object CodegenWholeStageTest {

  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark SQL Example")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
    benchmark("Spark 1.6") {
      spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").selectExpr("count(*)").explain(true)
    }

//    spark.sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
//    benchmark("Spark 2.0") {
//      spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").selectExpr("count(*)").explain(true)
//    }
  }
}
