/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.fxi.test.spark.ml

// $example on$
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types.{FloatType, IntegerType, ArrayType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating ALS.
 * Run with
 * {{{
 * bin/run-example ml.ALSExample
 * }}}
 */
object ALSExample {

  // $example on$
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  // $example off$

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[6]")
      .appName("ALSExample")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val ratings = spark.read.textFile("../spark/data/mllib/als/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)
    predictions.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    // $example off$
    val recDf = model.recommendForAllUsers(10)
    recDf.collect().foreach(println _)

    recDf.printSchema()
    recDf.show()

    val userRecommendDf = recDf.select("userId","recommendations.movieId")
    userRecommendDf.show()



    val userActionHistoryDf = ratings.select("userId","movieId").groupByKey(_.getAs[Int](0)).mapGroups((f1,f2) =>{
      T(f1,f2.map(_.getAs[Int]("movieId")).toArray[Int])
    })

    val df = userRecommendDf.join(userActionHistoryDf, userRecommendDf.col("userId") === userActionHistoryDf.col("userId"),
      "left_outer")
    df.show()
    df.map((f) =>{
       val userId = f.getAs[Int](0)
       var recommendArray = ArrayBuffer[Int]()
       recommendArray ++=f.getAs[mutable.WrappedArray[Int]](1)
       val actionHistoryArray = f.getAs[mutable.WrappedArray[Int]](3)
       recommendArray --= actionHistoryArray
       T(userId,recommendArray.toArray)
    }).show()


    spark.stop()
  }
}
// scalastyle:on println
case  class T(userId:Int,movieIds:Array[Int])
