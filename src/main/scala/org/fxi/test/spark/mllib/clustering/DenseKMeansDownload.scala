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
package org.fxi.test.spark.mllib.clustering

import org.fxi.test.spark.mllib.AbstractParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
  * An example k-means app. Run with
  * {{{
  * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
  * }}}
  * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
  */
object DenseKMeansDownload {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
                     input: String = "/home/xifei/data/ftp/summary/ad/app-20160401040204-5123/0/app-20160401040204-5123/3/96/downloadUsers",
                     k: Int = 20,
                     numIterations: Int = 20,
                     initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
          s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      opt[String]("<input>")
        .text("input paths to examples")
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DenseKMeans with $params").setMaster("local[*]")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = sc.textFile(params.input).map { line =>
      val downloadLogArr = line.split('\t')(3).split("#").reverse
      val arr = ArrayBuffer[Double]()
      //      println(line.split('\t')(3))


      for (i <- 0 until 50) {
        if (i <= downloadLogArr.size - 1) {
          try {
            arr += downloadLogArr(i).toDouble
          } catch {
            case e: Exception => println(downloadLogArr.size - 1 + " " + i)
          }
        } else {
          arr += 0.0
        }
      }

      Vectors.dense(arr.toArray)
    }.cache()

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(params.k)
      .setMaxIterations(params.numIterations)
      .run(examples)

    val cost = model.computeCost(examples)

    model.clusterCenters.foreach(println _)

    examples.sample(false,0.1).foreach( f=>{
     println( model.predict(f)+ "=>"+ f)

    })
//    model.predict(examples).foreach(println _)

    println(s"Total cost = $cost.")

    sc.stop()
  }
}

// scalastyle:on println
