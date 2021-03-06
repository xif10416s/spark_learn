package org.fxi.test.spark.stream.structured

import java.sql.Timestamp

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.scalatest.FunSuite

/**
  * Created by xifei on 16-10-12.
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * windows : https://eternallybored.org/misc/netcat/
  * nc.exe -l -p 9999
  */
class StructuredStreamTester extends FunSuite {


  test("Socket") {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    lines.isStreaming


    // Split the lines into words
    val newCol = () => {
      val t = new Timestamp(System.currentTimeMillis())
      println(t)
      t
    }
    val addCol = udf(newCol)
    val words = lines.as[String].flatMap(_.split(" ")).withColumn("time", addCol())
    words.printSchema()
    // 没有聚合操作，必须append
//    val query = words.writeStream.foreach(
//      new ForeachWriter[Row] {
//        override def open(partitionId: Long, epochId: Long): Boolean = {
//          true
//        }
//
//
//        override def process(value: Row): Unit = {
//          println(value)
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//
//        }
//      }).outputMode("append") // append , update ,complete
//      .format("console")
//      .start()


//     Generate running word count
        val wordCounts = words.withWatermark("time", "2 minutes").groupBy(window($"time", "2 minutes", "1 minutes")).count()

        val query = wordCounts.writeStream.foreach(new ForeachWriter[Row]{
          override def open(partitionId: Long, epochId: Long): Boolean = {
            true
          }

          override def process(value: Row): Unit = {
            println(value.get(0) +  " : " + value.get(1))
          }

          override def close(errorOrNull: Throwable): Unit = {

          }
        })
          .outputMode("append") // append , update ,complete
          .start()


    query.awaitTermination()
    Thread.sleep(10000L)
  }


  /**
    * 读取目录，新加文件会被处理
    */
  test("StructuredNetworkWordCount1") {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(userSchema) // Specify schema of the csv files
      .csv("data/structuredstream/") // Equivalent to format("csv").load("/path/to/directory")

    // Split the lines into words
    val words = csvDF.groupBy("name").count()
    // Generate running word count

    val query = words.writeStream
      .outputMode("complete")
      .format("console")
      .start()


    query.awaitTermination()
  }


  test("StructuredNetworkWordCount2") {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load().as[(String, Timestamp)]

    // Split the lines into words, retaining timestamps
    val words = lines.flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", "10 seconds", "5 seconds"), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}