package org.fxi.test.spark.stream.batch.base

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StateSpec, State, Seconds, StreamingContext}

import scala.collection.mutable.Queue

/**
  * Created by xifei on 16-4-7.
  * 1 local[*]
  * 1 spark://122.144.134.67:50002
  */
object QueueStreamWithStateful {
  def main(args: Array[String]): Unit = {
    val Array(batchDuration, master, windowLength, slidingInterval) = Array("1", "local[*]", "5", "5");
    //    val Array(batchDuration , master ,windowLength, slidingInterval) = Array("5","spark://122.144.134.67:50002" ,"10","10");

    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster(master)
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration.toInt))
    ssc.checkpoint("./scalaProject/checkpoint/")
    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue, false)
    val mappedStream = inputStream.map(x => ((x % 10).toString + "_" + new SimpleDateFormat("HH:mm").format(Calendar.getInstance().getTime), 1))

    // Update the cumulative count using mapWithState
    // This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      if (!state.exists()) {
        //load data
        val todayCnt = 20;
        state.update(todayCnt);
      }

      val sum = state.getOption.getOrElse(0) - one.getOrElse(0)
      if (sum <= 0) {
        //do some thing
        println("--------------------")
        println(word)
        println("--------------------")
      }
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = mappedStream.mapWithState(
      StateSpec.function(mappingFunc))
    stateDstream.print()
//    stateDstream.foreachRDD(rdd => {
//      println("************************************************")
//      rdd.foreachPartition(p => {
//        p.foreach(f => {
//          println(f._1 + " ==>" + f._2)
//        })
//      })
//    })

    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to 1000) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 10, 10)
      }
      Thread.sleep(1000)
    }
    //    ssc.stop()
    ssc.awaitTermination()
  }

}
