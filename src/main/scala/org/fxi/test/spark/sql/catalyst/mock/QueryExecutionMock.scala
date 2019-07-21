package org.fxi.test.spark.sql.catalyst.mock


import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.fxi.test.spark.sql.catalyst.mock.plans.logic.LogicalPlanMock

/**
  * Created by xifei on 16-10-14.
  */
class QueryExecutionMock(val sparkSession: SparkSessionMock, val logical: LogicalPlanMock) {
  def assertAnalyzed(): Unit = {
    try sparkSession.sessionState.analyzer.checkAnalysis(analyzed) catch {
      case e: Exception =>
        throw e
    }
  }

  lazy val analyzed: LogicalPlanMock = {
    LogUtil.doLog("＝＝＝＝遍历定义的规则解析计划＝＝＝＝＝开始"+logical.hashCode(),this.getClass)
    try{
      sparkSession.sessionState.analyzer.execute(logical)
    } finally {
      LogUtil.doLog("＝＝＝＝遍历定义的规则解析计划＝＝＝＝＝结束"+logical.hashCode(),this.getClass)
    }
  }

  lazy val withCachedData: LogicalPlanMock = {
    LogUtil.doLog("＝＝＝＝使用缓存数据＝＝＝＝＝＝＝＝＝＝＝",this.getClass)
    analyzed
  }

  lazy val optimizedPlan: LogicalPlanMock = sparkSession.sessionState.optimizer.execute(withCachedData)

  val planner = new SparkPlannerMock
  lazy val sparkPlan: SparkPlanMock = {
    LogUtil.doLog("＝＝＝＝将优化的逻辑计划转化成物理计划＝＝＝＝＝＝＝＝＝＝＝开始",this.getClass)
    val rs = planner.plan(optimizedPlan).next()
    LogUtil.doLog("＝＝＝＝将优化的逻辑计划转化成物理计划＝＝＝＝＝＝＝＝＝＝＝结束"+rs,this.getClass)
    rs
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlanMock = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
//  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

  /**
    * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
    * row format conversions as needed.
    */
  protected def prepareForExecution(plan: SparkPlanMock): SparkPlanMock = {
    LogUtil.doLog("＝＝＝＝准备执行物理计划，封装代码生成＝＝＝＝＝＝＝＝＝＝＝",this.getClass)
    val rs =preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
    println(rs)
    rs
  }


  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlanMock]] = Seq(
//    python.ExtractPythonUDFs,
//    PlanSubqueries(sparkSession),
//    EnsureRequirements(sparkSession.sessionState.conf),
      CollapseCodegenStagesMock(null)
//    ReuseExchange(sparkSession.sessionState.conf)
   )
}
