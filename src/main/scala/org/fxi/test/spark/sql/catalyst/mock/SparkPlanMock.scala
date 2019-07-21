package org.fxi.test.spark.sql.catalyst.mock

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.QueryPlan

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xifei on 16-10-17.
  */
abstract class SparkPlanMock extends QueryPlan[SparkPlanMock] with Serializable {
  final def execute(): RDD[InternalRow] = doExecute()

  protected def doExecute(): RDD[InternalRow]

  /**
    * Runs this query returning the result as an array.
    */
  def executeCollect(): Array[InternalRow] = {
    LogUtil.doLog("＝＝＝＝SparkPlan 执行收集任务＝＝＝＝＝＝＝＝＝＝＝",this.getClass)
    val byteArrayRdd = getByteArrayRdd()
    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect()
    LogUtil.doLog("＝＝＝＝将ｂｙｔｅ数组的ＲＤＤ转换成InternalRow的ｒｄｄ＝＝＝＝＝＝＝＝＝＝＝",this.getClass)
    results.toArray
  }


  private def getByteArrayRdd(n: Int = -1): RDD[Array[Byte]] = {
    LogUtil.doLog("＝＝＝＝SparkPlan 最后将计算结果InternalRow映射byte数组方便序列化传输　　　开始＝＝＝＝＝＝＝＝＝＝",this.getClass)

    execute().mapPartitions { iter =>
      println("2＝＝＝＝SparkPlan 执行将结果 internallRow 映射成 byte array＝＝＝＝＝＝＝＝＝＝")
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(bos)
      while (iter.hasNext && (n < 0 || count < n)) {
        println("4＝＝＝＝SparkPlan mapPartitions internallRow to byte array do iterator＝＝＝＝＝＝＝＝＝＝")
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      Iterator(bos.toByteArray)
    }
  }

}
