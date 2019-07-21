package org.fxi.test.spark.sql.catalyst.mock

import org.apache.spark.sql.catalyst.rules.Rule
import org.fxi.test.spark.sql.catalyst.mock.plans.logic.LogicalPlanMock

/**
  * Created by xifei on 16-10-14.
  */
class SparkOptimizerMock   extends RuleExecutorMock[LogicalPlanMock] {
  protected val fixedPoint = FixedPoint(10)
  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      LimitPushDown)
  )
}
object LimitPushDown extends Rule[LogicalPlanMock] {
  override def apply(plan: LogicalPlanMock): LogicalPlanMock = {
    LogUtil.doLog("Rule LimitPushDown ==> limit 优化",this.getClass)
    plan
  }
}
