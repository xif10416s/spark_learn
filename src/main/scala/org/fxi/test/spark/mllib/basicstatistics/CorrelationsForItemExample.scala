package org.fxi.test.spark.mllib.basicstatistics


import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.fxi.test.spark.utils.FileUtils

/**
  * Created by xifei on 16-6-1.
  */
object CorrelationsForItemExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CorrelationsExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val examples = sc.textFile("./scalaProject/scala-spark/data/test/userDownloadlog").map[(String, String)] {
      line => {
        val arr = line.split("\t")
        (arr(1), arr(0))
      }
    }.cache()

    val itemIndexRdd = examples.keys.distinct().zipWithIndex().sortBy(f => {
      f._2
    });
    FileUtils.saveToFile("./scalaProject/scala-spark/data/test/rs/index",itemIndexRdd.collect())
    itemIndexRdd.collect().foreach(println _)


    val svmFormat = examples.join(itemIndexRdd).map[(String, String)](f => {
      (f._2._1, (f._2._2+1).toString)
    }).reduceByKey((f1,f2)=>{f1+" "+f2}).map(f =>{
      val a = f._2.split(" ").map( f=> f.toInt)
      "1 " + a.sorted.mkString(":1 ")+":1"
    })

//      .groupByKey.map[String] {
//      f => {
//        val items = f._2.toList.sortBy(f =>f).mkString(":1 ")+":1"
//        s"1 $items"
//      }
//    }
    examples.cogroup(itemIndexRdd)

    svmFormat.collect().foreach(println _)
    FileUtils.saveToFile("./scalaProject/scala-spark/data/test/rs/svmRs",svmFormat.collect())
    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    // If a method is not specified, Pearson's method will be used by default.
    val datas = MLUtils.loadLibSVMFile(sc, "./scalaProject/scala-spark/data/test/rs/svmRs").map[Vector](f=>{
      f.features
    })
    val correlMatrix: Matrix = Statistics.corr(datas, "pearson")
//    println(correlMatrix.asInstanceOf[DenseMatrix].toString(1000,1000))
    println(correlMatrix.asInstanceOf[DenseMatrix].toSparse.toString())
    FileUtils.saveToFile("./scalaProject/scala-spark/data/test/rs/correlationsRs",correlMatrix.asInstanceOf[DenseMatrix].toSparse.toString(1000000,1000))
    sc.stop()
  }
}
