package org.fxi.test.spark.sql.catalyst.mock


import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.fxi.test.spark.sql.catalyst.mock.plans.logic.{FilterMock, LogicalPlanMock, ProjectMock}


object DataSetMock {
  def apply[T: Encoder](sparkSession: SparkSessionMock, logicalPlan: LogicalPlanMock): DataSetMock[T] = {
    new DataSetMock(sparkSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(sparkSession: SparkSessionMock, logicalPlan: LogicalPlanMock): DataFrameMock = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new DataSetMock[Row](sparkSession, logicalPlan, RowEncoder(qe.analyzed.schema))
  }
}
/**
  * Created by xifei on 16-10-14.
  */
class DataSetMock[T]( val sparkSession: SparkSessionMock,
                     val queryExecution: QueryExecutionMock,
                      encoder: Encoder[T]) {
  queryExecution.assertAnalyzed()

  private[sql] val logicalPlan: LogicalPlanMock = queryExecution.analyzed

  def this(sparkSession: SparkSessionMock, logicalPlan: LogicalPlanMock, encoder: Encoder[T]) = {
    this(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def select(cols: Column*): DataFrameMock ={
    LogUtil.doLog("执行查询操作select($\"age\" + 1,$\"name\")",this.getClass)
    withPlan {
      LogUtil.doLog("创建投影计划Ｐｒｏｊｅｃｔ",this.getClass)
      LogUtil.doLog("解析查询表达式，将表达式转换成Attribute",this.getClass)
      ProjectMock(cols, logicalPlan)
    }
  }

  @inline private def withPlan(logicalPlan: => LogicalPlanMock): DataFrameMock = {
    LogUtil.doLog("将logic Project计划生成ＤａｔａＦｒａｍｅ（StructType 为Ｒｏｗ）",this.getClass)
    DataSetMock.ofRows(sparkSession, logicalPlan)
  }

  def filter(condition: Column): DataSetMock[T] = {
    LogUtil.doLog("执行过滤操作filter($\"(age + 1)\" > 20)",this.getClass)
    withTypedPlan {
      LogUtil.doLog("解析过滤条件，生成过滤计划Ｆｉｌｔｅｒ",this.getClass)
      FilterMock(null, logicalPlan)
    }
  }


  /** A convenient function to wrap a logical plan and produce a Dataset. */
  @inline private def withTypedPlan[U](logicalPlan: => LogicalPlanMock): DataSetMock[T] = {
    LogUtil.doLog("将logicＦｉｌｔｅｒ　计划生成DataSet　（StructType）------------",this.getClass)
    new DataSetMock(sparkSession, logicalPlan, encoder)
  }

  def collect(): Array[T]= {
    queryExecution.executedPlan.executeCollect()
    LogUtil.doLog("将收集的Array[InternalRow]通过boundEnc转换成对象------------",this.getClass)
    null
  }
}
