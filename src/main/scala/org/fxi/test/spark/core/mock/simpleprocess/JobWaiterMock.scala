package org.fxi.test.spark.core.mock.simpleprocess

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging

import scala.concurrent.{Future, Promise}

/**
  * Created by seki on 17/10/10.
  */
class JobWaiterMock[T](
                  dagScheduler: DAGSchedulerMock,
                  val jobId: Int,
                  totalTasks: Int,
                  resultHandler: (Int,  T) => Unit) extends Logging {

  private val finishedTasks = new AtomicInteger(0)
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()

  def jobFinished: Boolean = jobPromise.isCompleted

  def completionFuture: Future[Unit] = jobPromise.future


  def taskSucceeded(index: Int, result: Any): Unit = {
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized {
      println(s"任务${index}完成, JobWaiter 调用mergeResult函数合并成功task结果[$result]")
      resultHandler(index, result.asInstanceOf[T])
    }
    if (finishedTasks.incrementAndGet() == totalTasks) {
      println("所有任务完成...")
      jobPromise.success(())
    }
  }

}
