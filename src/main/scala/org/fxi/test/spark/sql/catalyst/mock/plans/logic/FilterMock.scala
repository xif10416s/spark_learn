package org.fxi.test.spark.sql.catalyst.mock.plans.logic

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute}

/**
  * Created by xifei on 16-10-14.
  */
case class FilterMock(condition: Expression, child: LogicalPlanMock)
  extends LogicalPlanMock {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[LogicalPlanMock] = Nil
}
