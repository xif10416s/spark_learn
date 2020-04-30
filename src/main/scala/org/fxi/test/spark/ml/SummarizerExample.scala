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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer
// $example off$
import org.apache.spark.sql.SparkSession

object SummarizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SummarizerExample")
      .master("local[*]")
      .getOrCreate()

    import Summarizer._
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // $example on$
    val data = Seq(
      (Vectors.dense(2.0, 3.0, 11.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0),
      (Vectors.dense(-1, 6.0, 7.0), 3.0),
      (Vectors.dense(0, 6.0, 7.0), 4.0)
    )

    val df = data.toDF("features", "weight")



    val (meanVal, varianceVal) = df.select(metrics("mean", "variance")
      .summary($"features", $"weight").as("summary"))
      .select("summary.mean", "summary.variance")
      .as[(Vector, Vector)].first()

    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

    val (meanVal2, varianceVal2, count2) = df.select(mean($"features"), variance($"features"), max($"features"))
      .as[(Vector, Vector,Vector)].first()


    println(s"without weight: mean = ${meanVal2}, variance = ${varianceVal2}, count = ${count2}")
    // $example off$

    println( df.select(min($"features"),max($"features"),mean($"features"),numNonZeros($"features"), variance($"features"),normL1($"features"),count($"features"))
      .as[(Vector,Vector, Vector,Vector,Vector,Vector,Long)].first())

    df.stat.approxQuantile("weight",Array(0.1,0.5,0.7),0.01).foreach(println _)
    spark.stop()

    df.select().stat.approxQuantile("weight",Array(0.1,0.5,0.7),0.01)
  }
}
// scalastyle:on println
