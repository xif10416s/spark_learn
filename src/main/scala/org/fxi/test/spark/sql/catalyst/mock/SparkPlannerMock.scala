package org.fxi.test.spark.sql.catalyst.mock


import org.fxi.test.spark.sql.catalyst.mock.plans.logic.LogicalPlanMock


/**
  * Created by xifei on 16-10-14.
  * 将逻辑计划转换物理计划
  */
class SparkPlannerMock extends SparkStrategiesMock {
   def strategies: Seq[SparkStrategyMock]={
    Seq(SpecialLimits ,Aggregation,JoinSelection,BasicOperators)
  }

  def plan2(plan: LogicalPlanMock): Iterator[SparkPlanMock] = {
    // Obviously a lot to do here still...
    val iter = strategies.view.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }

   def plan(plan: LogicalPlanMock): Iterator[SparkPlanMock] = {
     plan2(plan).map {
      _.transformUp {
        case PlanLaterMock(p) =>
          // TODO: use the first plan for now, but we will implement plan space exploaration later.
          this.plan(p).next()
      }
    }
  }
}
