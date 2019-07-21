package org.fxi.test.spark.core.mock.simpleprocess

import scala.math._

/**
  * Created by seki on 17/10/10.
  */
object Main {
  def main(args: Array[String]) {
    println("STEP 1 : 准备map函数 , 准备reduce函数 , 准备数据,设置切片数量")
    val mapFunc : Int => Int = {
        i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y <= 1) 1 else 0
    }
    val reduceFunc: (Int, Int) => Int = {
      _+_
    }

    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt

    val dsm = new DAGSchedulerMock(slices ,new MapPartitionsRDDMock[Int,Int](1 until n,( pid, iter) => iter.map(mapFunc) ,slices))
    val jobResult = dsm.run(reduceFunc)
    println("Pi is roughly " + 4.0 *jobResult / (n - 1))
  }
}
