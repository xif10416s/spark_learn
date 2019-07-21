package org.fxi.test.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.io.Source

class RddTester extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession = null

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  test("group by") {
    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val groupRdd = spark.sparkContext.parallelize(1 until n, slices)
      .groupBy(f => f)
    groupRdd.collect()
      .foreach(println _)
  }


  test("testCount ") {
    val rdd1: RDD[Int] = spark.sparkContext.makeRDD(1 to 1000, 10)
    println(rdd1.count())
  }

  test("testMapPartition ") {
    val rs = spark.sparkContext.makeRDD(1 to 1000, 10).mapPartitions(f => {
      f.map[Int](i => {
        1000 + i
      }).filter(f => f % 2 == 0)
    })

    println(rs.collect().mkString(","))
  }

  test("testForEachPartition ") {
    spark.sparkContext.makeRDD(1 to 1000, 10).foreachPartition(
      f => {
        f.foreach(item => {
          println(item)
        })
      }
    )

  }

  test("testFileToRDD ") {
    val userArray = for (line <- Source.fromFile("./data/test/userAndAd").getLines()) yield line
    spark.sparkContext.parallelize(userArray.toSeq).foreach(println(_))

  }


  def testCache(): Unit = {
    val df = spark.sparkContext.textFile("./data/test/userAndAd").map[String](f => {
      println(f)
      f
    })

    val df2 = spark.sparkContext.textFile("./data/test/userAndAd").map[String](f => {
      println("2 --" + f)
      f
    })

    val d = df.intersection(df2)

    println(d.count())
    println("------------------")
    println(d.count())
    println("==================")
    d.cache() // 不用赋值
    //    println(df.countByValue())
    //    println("------------------")
    //    println(df.countByValue())
    println("------------------")
    d.repartition(10).foreachPartition(f => {
      f.foreach(i => {
        println(s"c - >$i ")
      })
    })
    println(d.collect())
  }


  def testZip(): Unit = {
    val a = spark.sparkContext.makeRDD(Array("a", "b", "c"))
    val b = spark.sparkContext.makeRDD(Array("1", "2", "3"))
    a.zip(b).foreach(println _)
  }


  def testCountByKey(): Unit = {
    val a = spark.sparkContext.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("c", 1), ("d", 1), ("d", 1), ("d", 1)))
    a.countByKey().toList.sortBy(f => f._2).reverse.take(3).foreach(println _)
    a.map(f => (f._1, 1)).reduceByKey(_ + _).map[(String, Long)](f => {
      (f._1, f._2)
    }).sortBy(f => f._2, false).zipWithIndex().filter(f => f._2 <= 2).map[(String, (Long, Long))](f => (f._1._1, (f._1._2, f._2))).collect().foreach(println _)
  }


  def testCogroup(): Unit = {
    val a = spark.sparkContext.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("c", 1), ("d", 1), ("d", 1), ("d", 1)))
    val b = spark.sparkContext.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("c", 1), ("d", 1), ("d", 1), ("d", 1), ("e", 1)))
    val c = spark.sparkContext.makeRDD(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("c", 2), ("d", 1), ("d", 1), ("d", 1), ("e", 1)))
    a.cogroup(b).foreach(println _)
    val arr = Array(a, b, c)
    val a1 = arr(0)
    for (i <- 1 until arr.size) {
      a1.leftOuterJoin(arr(i))
    }
  }


  def testGroup(): Unit = {
    val a = spark.sparkContext.makeRDD(Array(("a", 1), ("b", 1), ("a", 2), ("a", 3), ("c", 1), ("d", 1), ("d", 2), ("d", 3)))
    a.map(f => (f._1, f._2.toString)).reduceByKey((f1, f2) => {
      println(f1 + "--" + f2); f1 + ":1 " + f2
    }).map(f => (f._1, f._2 + ":1")).foreach(println _)
  }





  def testReduce(): Unit = {
    val a = spark.sparkContext.makeRDD(Array((1, 1), (2, 1), (3, 2)))
    println(a.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2)))
  }



  def testGroup2(): Unit = {
    val a = spark.sparkContext.makeRDD(Array(("a", 1), ("a", 1), ("b", 1), ("a", 2), ("a", 3), ("c", 1), ("d", 1), ("d", 2), ("d", 3)))

    a.distinct().map[(Int, Int)](f => (f._2, 1)).countByKey().foreach(println _)
  }

}
