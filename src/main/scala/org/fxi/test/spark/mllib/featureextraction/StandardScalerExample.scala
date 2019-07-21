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
package org.fxi.test.spark.mllib.featureextraction


import org.apache.spark.{SparkConf, SparkContext}
import org.fxi.test.spark.mllib.KeyedPoint

// $example on$
import org.apache.spark.mllib.feature.{Normalizer, StandardScaler, StandardScalerModel}

// $example off$

object StandardScalerExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StandardScalerExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // $example on$
    //    val data = MLUtils.loadLibSVMFile(sc, "./spark/data/mllib/sample_libsvm_data.txt")

    //    val scaler1 = new StandardScaler().fit(data.map(x => x.features))
    //    val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(data.map(x => x.features))
    //    // scaler3 is an identical model to scaler2, and will produce identical transformations
    //    val scaler3 = new StandardScalerModel(scaler2.std, scaler2.mean)
    //    // data1 will be unit variance.
    //    val data1 = data.map(x => (x.label, scaler1.transform(x.features)))
    //
    //    // Without converting the features into dense vectors, transformation with zero mean will raise
    //    // exception on sparse vector.
    //    // data2 will be unit variance and zero mean.
    //    val data2 = data.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))

    // $example off$

    val path = "./scalaProject/scala-spark/data/test/userInfo"
    val data = sc.textFile(path).map(KeyedPoint.parse).cache()

    val scaler1 = new StandardScaler().fit(data.map(x => x.features))
    val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(data.map(x => x.features))
    // scaler3 is an identical model to scaler2, and will produce identical transformations
    val scaler3 = new StandardScalerModel(scaler2.std, scaler2.mean)
    // data1 will be unit variance.
    val data1 = data.map(x => (x.label,scaler1.transform(x.features)))

    // Without converting the features into dense vectors, transformation with zero mean will raise
    // exception on sparse vector.
    // data2 will be unit variance and zero mean.
    val data2 = data.map(x => (x.label,scaler2.transform(x.features)))


    println("data1: ")
    data1.sortByKey().collect().foreach(x => println(x._1 +" , " + x._2.toArray.map(f => "%.3f".format(f)).mkString("\t")))

    println("data2: ")
    data2.sortByKey().collect().foreach(x => println(x._1 +" , " + x._2.toArray.map(f => "%.3f".format(f)).mkString("\t")))

    //
    val normalizer1 = new Normalizer()
    val normalizer2 = new Normalizer(p = Double.PositiveInfinity)

    // Each sample in data1 will be normalized using $L^2$ norm.
    val data11 = data1.map(x => (x._1, normalizer1.transform(x._2)))

    // Each sample in data2 will be normalized using $L^\infty$ norm.
    val data22 = data1.map(x => (x._1, normalizer2.transform(x._2)))
    // $example off$
//
//    println("data1: normal ")
//    data11.sortByKey().collect().foreach(x => println(x._1 +" , " + x._2.toArray.map(f => "%.3f".format(f)).mkString("\t")))
//
//    println("data2: normal ")
//    data22.sortByKey().collect().foreach(x => println(x._1 +" , " + x._2.toArray.map(f => "%.3f".format(f)).mkString("\t")))

    sc.stop()
  }
}

// scalastyle:on println
