package org.fxi.test.spark.sql.datasets

import org.apache.spark.sql.SparkSession


import scala.math._

/**
  * Created by xifei on 16-8-15.
  */
class BaseDatasetsTaster {
  val spark = SparkSession
    .builder
    .appName("Spark SQL Example")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._


  def test01() = {

    val df = spark.read.json("../../spark/main/resources/people.json")
    val qf = df.filter("age > 15").select($"age" + 1).limit(100)
    println(qf.queryExecution.logical)
    qf.collect().foreach(println _)
    df.createOrReplaceTempView("people")
//    val query = spark.sql("select * from  (select * from people  where name is not null limit 10) p1 join people p2 on p1.name = p2.name where p1.age > 19 limit 100 ")
    val query = spark.sql("select * from people where age > 19 limit 100 ")

    query.collect()
    println("+++++++++++++++++++logical plan ++++++++++++++++++")
    println(query.queryExecution.logical)
    println("+++++++++++++++++++analyzed plan ++++++++++++++++++")
    println( query.queryExecution.analyzed)
    println("+++++++++++++++++++withCachedData plan ++++++++++++++++++")
    println( query.queryExecution.withCachedData)
    println("+++++++++++++++++++optimizedPlan plan ++++++++++++++++++")
    println( query.queryExecution.optimizedPlan)
    println("+++++++++++++++++++sparkPlan plan ++++++++++++++++++")
    println( query.queryExecution.sparkPlan)
    println("+++++++++++++++++++executedPlan plan ++++++++++++++++++")
    println( query.queryExecution.executedPlan)
    //    df.show()
//    df.printSchema()
//    val ds = spark.read.json("../../spark/main/resources/people.json").as[Person]
//    ds.filter($"age">19).show()
//    println(ds.schema)
    spark.stop()
  }


  def test02() = {
    import spark.implicits._
    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32),Person("jude", 20)).toDS()
    val query = caseClassDS.select($"age" + 1,$"name").filter($"(age + 1)" > 20)
    query.explain()
//    println("+++++++++++++++++++logical plan ++++++++++++++++++")
//    println(query.queryExecution.logical)
//    println("+++++++++++++++++++analyzed plan ++++++++++++++++++")
//    println( query.queryExecution.analyzed)
//    println("+++++++++++++++++++withCachedData plan ++++++++++++++++++")
//    println( query.queryExecution.withCachedData)
//    println("+++++++++++++++++++optimizedPlan plan ++++++++++++++++++")
//    println( query.queryExecution.optimizedPlan)
//    println("+++++++++++++++++++sparkPlan plan ++++++++++++++++++")
//    println( query.queryExecution.sparkPlan)
//    println("+++++++++++++++++++executedPlan plan ++++++++++++++++++")
//    println( query.queryExecution.executedPlan)
//    println(query.count() )
  }
}

