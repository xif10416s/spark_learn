package org.fxi.test.spark.sql.catalyst.mock

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.fxi.test.spark.sql.catalyst.mock.exectution.{FilterExecMock, LocalTableScanExecMock, ProjectExecMock}
import org.fxi.test.spark.sql.catalyst.mock.plans.logic.{FilterMock, LocalRelationMock, LogicalPlanMock, ProjectMock}

/**
  * Created by xifei on 16-10-17.
  */
abstract class SparkStrategiesMock {
  self: SparkPlannerMock =>

  object SpecialLimits extends SparkStrategyMock {
    override def apply(plan: LogicalPlanMock): Seq[SparkPlanMock] = {
      LogUtil.doLog("SparkStrategiesMock SpecialLimits ==> limit 物理转换", this.getClass)
      Nil
    }
  }

  object JoinSelection extends SparkStrategyMock {
    override def apply(plan: LogicalPlanMock): Seq[SparkPlanMock] = {
      LogUtil.doLog("SparkStrategiesMock JoinSelection ==> 选择合适的ｊｏｉｎ方式", this.getClass)
      Nil
    }
  }

  object Aggregation extends SparkStrategyMock {
    override def apply(plan: LogicalPlanMock): Seq[SparkPlanMock] = {
      LogUtil.doLog("SparkStrategiesMock Aggregation ==> 选择合适的Aggregation方式", this.getClass)
      Nil
    }
  }

  object BasicOperators extends SparkStrategyMock {
    override def apply(plan: LogicalPlanMock): Seq[SparkPlanMock] = {
      LogUtil.doLog("SparkStrategiesMock BasicOperators ==> BasicOperators", this.getClass)
      plan match {
        case ProjectMock(projectList, child) =>
          ProjectExecMock(projectList, planLater(child)) :: Nil
        case FilterMock(null, child) =>
          FilterExecMock(null, planLater(child)) :: Nil
        case e:LocalRelationMock =>
          LocalTableScanExecMock() :: Nil
      }
    }

  }

}

abstract class GenericStrategyMock[PhysicalPlan <: TreeNode[PhysicalPlan]] {

  /**
    * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
    * filled in automatically by the QueryPlanner using the other execution strategies that are
    * available.
    */
  protected def planLater(plan: LogicalPlanMock): PhysicalPlan

  def apply(plan: LogicalPlanMock): Seq[PhysicalPlan]
}


abstract class QueryPlannerMock[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategyMock[PhysicalPlan]]

  def plan(plan: LogicalPlanMock): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.view.flatMap(_ (plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}

abstract class SparkStrategyMock extends GenericStrategyMock[SparkPlanMock] {

  override protected def planLater(plan: LogicalPlanMock): SparkPlanMock = PlanLaterMock(plan)
}

case class PlanLaterMock(plan: LogicalPlanMock) extends SparkPlanMock {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  override def children: Seq[SparkPlanMock] = Nil
  override def producedAttributes: AttributeSet = outputSet
}
