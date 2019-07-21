package org.fxi.test.spark.core.mock.simpleprocess

import java.util.concurrent.{Executors, ExecutorService}

import org.apache.spark.internal.Logging

import scala.concurrent.duration.Duration

/**
  * Created by seki on 17/10/10.
  */
class DAGSchedulerMock(val partionNum:Int ,val rdd: MapPartitionsRDDMock[Int,Int]) extends Logging {
  def run(reduceFunc : (Int, Int) => Int):Int = {
    println("STEP 2 : 根据reduce函数创建partition内数据的reduce函数reducePartition")
    val reducePartition: Iterator[Int] => Option[Int] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(reduceFunc))
      } else {
        None
      }
    }
    var jobResult: Option[Int] = None

    println("STEP 3 : 根据reduce函数创建partition的reduce函数mergeResult")
    val mergeResult = (index: Int, taskResult: Option[Int]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => {
            println(s"partition:{$index} 合并任务结果开始..")
            Some(reduceFunc(value, taskResult.get))
          }
          case None => taskResult
        }
      }
    }

    println("STEP 3 :  创建JobWaiter等待并监听任务执行成功结果")
    val jobwaiter = new JobWaiterMock[Option[Int]](this,1,partionNum,mergeResult)

    val  es = Executors.newFixedThreadPool(partionNum)
    println("STEP 4 :  提交任务执行")
    for( i <- 0 until  partionNum) {
      es.execute(new Runnable {
        override def run(): Unit = {
          Thread.sleep(1000L)
          println(s"线程$i rdd.compute 并 调用reducePartition,合并单个partition结果 ")
          val result = reducePartition(rdd.compute(i))
          println(s"线程$i 触发jobwaiter任务完成 返回结果 $result")
          jobwaiter.taskSucceeded(i, result)

        }
      })
    }

    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    println("STEP 5 : 通过jobwaiter,主线程进入等待状态 ")
    jobwaiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    println("STEP 6 : 任务全部完成,返回结果 ")
    jobResult.get
  }
}
