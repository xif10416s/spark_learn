package org.fxi.test.spark.stream.batch.base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

/**
  * Created by xifei on 16-4-7.
  * 1 local[*]
  * 1 spark://122.144.134.67:50002
  */
object QueueStream {
  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    val logLevel = Level.DEBUG
    Logger.getLogger("org").setLevel(logLevel)

    val Array(batchDuration , master ,windowLength, slidingInterval) = Array("5","local[*]" ,"5","5");
//    val Array(batchDuration , master ,windowLength, slidingInterval) = Array("5","spark://122.144.134.67:50002" ,"10","10");

    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster(master)
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration.toInt))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue,false)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    val reduceByWindow = mappedStream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(windowLength.toInt), Seconds(slidingInterval.toInt))
    reduceByWindow.print();
    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to 100) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 10000000, 10)
      }
      Thread.sleep(1000)
    }
//    ssc.stop()
    ssc.awaitTermination()
  }

}
