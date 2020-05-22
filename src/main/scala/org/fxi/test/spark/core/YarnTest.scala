package org.fxi.test.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class YarnTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession = null

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("yarn")
      .config("spark.submit.deployMode","cluster")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  test("testCount ") {
    val rdd1: RDD[Int] = spark.sparkContext.makeRDD(1 to 1000, 10)
    println(rdd1.count())
  }
}
