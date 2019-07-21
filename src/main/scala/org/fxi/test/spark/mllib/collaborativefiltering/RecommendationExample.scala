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
package org.fxi.test.spark.mllib.collaborativefiltering

import org.apache.spark.mllib.evaluation.{RegressionMetrics, RankingMetrics}
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
// $example off$

object RecommendationExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data
    val data = sc.textFile("./spark/data/mllib/als/test.data")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)


    //--------------
    // Map ratings to 1 or 0, 1 indicating a movie that should be recommended
    val binarizedRatings = ratings.map(r => Rating(r.user, r.product,
      if (r.rating > 0) 1.0 else 0.0)).cache()
    // Define a function to scale ratings from 0 to 1
    def scaledRating(r: Rating): Rating = {
      val scaledRating = math.max(math.min(r.rating, 1.0), 0.0)
      Rating(r.user, r.product, scaledRating)
    }

    // Get sorted top ten predictions for each user and then scale from [0, 1]
    val userRecommended = model.recommendProductsForUsers(10).map { case (user, recs) =>
      (user, recs.map(scaledRating))
    }

    // Assume that any movie a user rated 3 or higher (which maps to a 1) is a relevant document
    // Compare with top ten most relevant documents
    val userMovies = binarizedRatings.groupBy(_.user)
    val relevantDocuments = userMovies.join(userRecommended).map { case (user, (actual,
    predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray)
    }

    // Instantiate metrics object
    val metrics = new RankingMetrics(relevantDocuments)

    // Precision at K
    Array(1, 3, 5).foreach { k =>
      println(s"Precision at $k = ${metrics.precisionAt(k)}")
    }

    // Mean average precision
    println(s"Mean average precision = ${metrics.meanAveragePrecision}")

    // Normalized discounted cumulative gain
    Array(1, 3, 5).foreach { k =>
      println(s"NDCG at $k = ${metrics.ndcgAt(k)}")
    }

    // Get predictions for each data point
    val allPredictions = model.predict(ratings.map(r => (r.user, r.product))).map(r => ((r.user,
      r.product), r.rating))
    val allRatings = ratings.map(r => ((r.user, r.product), r.rating))
    val predictionsAndLabels = allPredictions.join(allRatings).map { case ((user, product),
    (predicted, actual)) =>
      (predicted, actual)
    }

    // Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
    println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${regressionMetrics.r2}")

    // Save and load model
//    model.save(sc, "target/tmp/myCollaborativeFilter")
//    val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
    // $example off$
  }
}
// scalastyle:on println
