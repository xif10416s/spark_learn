package org.fxi.test.spark.sql.catalyst.mock.plans.logic

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
  * Created by xifei on 16-10-14.
  */
case class ProjectMock(projectList: Seq[Column], child: LogicalPlanMock)  extends LogicalPlanMock  {

  override def output: Seq[Attribute] = Nil

  override def children: Seq[LogicalPlanMock] = child :: Nil

}
