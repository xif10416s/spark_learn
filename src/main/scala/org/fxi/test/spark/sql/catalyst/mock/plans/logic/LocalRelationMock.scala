package org.fxi.test.spark.sql.catalyst.mock.plans.logic

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * Created by xifei on 16-10-14.
  */
case class LocalRelationMock(output: Seq[Attribute], data: Seq[InternalRow] = Nil) extends LogicalPlanMock {
  override def children: Seq[LogicalPlanMock] = Nil
}
