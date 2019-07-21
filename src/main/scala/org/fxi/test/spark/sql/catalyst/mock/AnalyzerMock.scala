package org.fxi.test.spark.sql.catalyst.mock


import org.apache.spark.sql.catalyst.rules.Rule
import org.fxi.test.spark.sql.catalyst.mock.plans.logic.LogicalPlanMock

/**
  * Created by xifei on 16-10-14.
  */
class AnalyzerMock extends RuleExecutorMock[LogicalPlanMock] {
  protected val fixedPoint = FixedPoint(10)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution),
    Batch("Resolution", fixedPoint,
      ResolveRelations, ResolveReferences)
  )

  def checkAnalysis(plan: LogicalPlanMock): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    LogUtil.doLog("检查解析结果＝＝＝＝================＝开始"+plan.hashCode(),this.getClass)
    plan.foreachUp {
      case _ =>
      LogUtil.doLog("检查没有解析的关系",this.getClass)
      LogUtil.doLog("检查没有解析的属性",this.getClass)
      LogUtil.doLog("检查不匹配的参数类型",this.getClass)
      LogUtil.doLog("各种基础检查．．．",this.getClass)
    }
    LogUtil.doLog("检查解析结果＝＝＝＝================＝结束"+plan.hashCode(),this.getClass)
  }


}

object CTESubstitution extends Rule[LogicalPlanMock] {
  override def apply(plan: LogicalPlanMock): LogicalPlanMock = {
    LogUtil.doLog("Rule CTESubstitution ==> CTESubstitution  替换",this.getClass)
    plan
  }
}


object ResolveRelations extends Rule[LogicalPlanMock] {
  override def apply(plan: LogicalPlanMock): LogicalPlanMock = {
    LogUtil.doLog("Rule ResolveRelations ==> 解析关联关系，从catalog中查找表，字段是否存在",this.getClass)
    plan
  }
}

/**
  * Replaces 　UnresolvedAttributes with concrete　　AttributeReference]]s from
  * a logical plan node's children.
  */
object ResolveReferences extends Rule[LogicalPlanMock] {
  override def apply(plan: LogicalPlanMock): LogicalPlanMock = {
    LogUtil.doLog("Rule ResolveReferences ==> 从逻辑计划的字节点，使用具体的属性替换未解析的属性",this.getClass)
    plan
  }
}

