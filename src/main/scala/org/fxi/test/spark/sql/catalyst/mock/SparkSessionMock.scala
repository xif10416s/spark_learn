package org.fxi.test.spark.sql.catalyst.mock

import javassist.bytecode.stackmap.TypeTag

import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.fxi.test.spark.sql.catalyst.mock.plans.logic.LocalRelationMock

/**
  * Created by xifei on 16-10-14.
  */
class SparkSessionMock {
   lazy val sessionState: SessionStateMock = new SessionStateMock(this)

  def createDataset[T : Encoder](data: Seq[T]): DataSetMock[T] = {
    LogUtil.doLog("通过内置ExpressionEncoder　转换case class Person为StructType，以及提供序列化和反序列化spark row",this.getClass)
    val enc =  encoderFor[T]
    val attributes = enc.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    LogUtil.doLog("将原始数据通过encoder转换成InternalRow[UnsafeRow]<==spark的内部数据结构,提高序列化速度,自己管理内存处理",this.getClass)
    val encoded = data.map(d => enc.toRow(d).copy())
    LogUtil.doLog("将InternalRow数据对象和schema转换的属性对象封装成LocalRelation，叶子逻辑计划",this.getClass)
    val plan = new LocalRelationMock(attributes, encoded)
    LogUtil.doLog("叶子逻辑计划LocalRelation　封装成DataSet",this.getClass)
    DataSetMock[T](this,plan)
  }
}
