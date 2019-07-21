package org.fxi.test.spark.stream.structured

import java.sql.Timestamp

import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, ForeachWriter, SparkSession}

/**
  * Created by xifei on 16-10-12.
  */
class StructuredStreamTester {


  def test01() = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    lines.isStreaming

    lines.printSchema()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream.foreach(new ForeachWriter[Row] {
      override def process(value: Row): Unit = {
        println("------------")
        println(value)
      }

      override def close(errorOrNull: Throwable): Unit = {

      }

      override def open(partitionId: Long, version: Long): Boolean = {
        true
      }

    })
      .outputMode("complete") // append , update ,complete
      .format("foreach")
      .start()


    query.awaitTermination()
  }



  def test02() = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("/tmp/test")    // Equivalent to format("csv").load("/path/to/directory")

    // Split the lines into words
    val words = csvDF.groupBy("name").count()
    // Generate running word count

    val query = words.writeStream.foreach(new ForeachWriter[Row] {
      override def process(value: Row): Unit = {
        println("------------")
        println(value)
      }

      override def close(errorOrNull: Throwable): Unit = {

      }

      override def open(partitionId: Long, version: Long): Boolean = {
        true
      }

    })
      .outputMode("complete")
      .format("foreach")
      .start()


    query.awaitTermination()
  }



  def test03() = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._


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
