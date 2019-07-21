package org.fxi.test.scala.core.collection

import org.apache.spark.rdd.RDD


import scala.collection.mutable.{Queue, ArrayBuffer}

/**
  * Created by xifei on 16-4-8.
  */
class QueueTest {

  def test(): Unit = {
    println("aaa")
  }


  def testPlus(): Unit = {
    println(initQueue())
  }


  def testDequeue(): Unit = {
    val buffer = new ArrayBuffer[Int]()
    val queue = initQueue();
    buffer += queue.dequeue()
    println(buffer.head)
    Some(buffer.head)
  }

  def initQueue(): Queue[Int] = {
    val rddQueue = new Queue[Int]()
    for (i <- 1 to 100) {
      rddQueue.synchronized {
        rddQueue += i
      }
    }
    rddQueue
  }
}
