package org.fxi.test.delta

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class BaseTester extends FunSuite {
  val spark = SparkSession
    .builder
    .appName("Spark Pi")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  test("basic save") {
    val data = spark.range(0, 5)
    data.write.format("delta").save("./tmp/delta-table")


  }

  test("basic update") {
    val data = spark.range(5, 10)
    data.write.format("delta").mode("overwrite").save("./tmp/delta-table")
  }

  test("basic read") {
    val df = spark.read.format("delta").load("./tmp/delta-table")
    df.show()
  }

  test("Time Travel") {
    val df = spark.read.format("delta").option("versionAsOf", 0).load("./tmp/delta-table")
    df.show()
  }

  test("stream") {
    //以每秒指定的行数生成数据，每个输出行包含一个timestamp和value。其中timestamp是一个Timestamp含有信息分配的时间类型，并且value是Long包含消息的计数从0开始作为第一行类型。此源用于测试和基准测试。
    val streamingDf = spark.readStream.format("rate").load()

    val stream = streamingDf.select($"value" as "id").writeStream.format("delta").option("checkpointLocation", "./tmp/checkpoint").start("./tmp/delta-table")

  }
 }
