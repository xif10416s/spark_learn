package org.fxi.test.spark.sql.catalyst.mock.exectution


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.fxi.test.spark.sql.catalyst.mock.{Driver, LogUtil, SparkPlanMock}

/**
  * Created by xifei on 16-10-17.
  */
case class LocalTableScanExecMock() extends SparkPlanMock  with CodegenSupportMock{
  override def children: Seq[SparkPlanMock] = Nil

  override def doExecute(): RDD[InternalRow] = {
    LogUtil.doLog("＝＝＝＝LocalTableScanExecMock　doExecute　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)
    LogUtil.doLog("＝＝＝＝LocalTableScanExecMock　doExecute　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝结束＝＝＝＝＝＝＝＝＝＝",this.getClass)
    Driver.spark.sparkContext.emptyRDD
  }


  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    execute() :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    LogUtil.doLog("＝＝＝＝LocalTableScanExecMock　doProduce　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)
    val input = ctx.freshName("input")
    // Right now, InputAdapter is only used when there is one input RDD.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val row = ctx.freshName("row")
    LogUtil.doLog("＝＝＝＝LocalTableScanExecMock　doProduce　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝结束＝＝＝＝＝＝＝＝＝＝",this.getClass)

    s"""
       | while ($input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consume(ctx, null, row).trim}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
  }

  override def output: Seq[Attribute] = Nil
}
