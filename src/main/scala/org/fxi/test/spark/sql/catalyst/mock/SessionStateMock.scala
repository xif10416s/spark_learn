package org.fxi.test.spark.sql.catalyst.mock


import org.fxi.test.spark.sql.catalyst.mock.plans.logic.LogicalPlanMock

/**
  * Created by xifei on 16-10-14.
  */
class SessionStateMock(sparkSession: SparkSessionMock) {
  lazy val analyzer: AnalyzerMock = new AnalyzerMock()
  lazy val optimizer: SparkOptimizerMock = new SparkOptimizerMock()

  def planner: SparkPlannerMock =
    new SparkPlannerMock()

  def executePlan(plan: LogicalPlanMock): QueryExecutionMock = {
    LogUtil.doLog("将logicPlan转换为QueryExecution",this.getClass)
    new QueryExecutionMock(sparkSession, plan)
  }
}
