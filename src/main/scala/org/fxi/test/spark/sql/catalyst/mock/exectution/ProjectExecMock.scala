package org.fxi.test.spark.sql.catalyst.mock.exectution


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.execution.CodegenSupport
import org.fxi.test.spark.sql.catalyst.mock.{LogUtil, SparkPlanMock}

/**
  * Created by xifei on 16-10-17.
  */
case class ProjectExecMock(condition: Seq[Column], child: SparkPlanMock)
  extends SparkPlanMock with CodegenSupportMock{
  // Split out all the IsNotNulls from condition.

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    Nil
  }


  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    LogUtil.doLog("＝＝＝＝ProjectExecMock　doProduce　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)
    val rs = child.asInstanceOf[CodegenSupportMock].produce(ctx, this)
    LogUtil.doLog("＝＝＝＝ProjectExecMock　doProduce　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝结束＝＝＝＝＝＝＝＝＝＝",this.getClass)
    rs

  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    LogUtil.doLog("＝＝＝＝ProjectExecMock　doConsume　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)

    /**
      * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
      */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    ctx.currentVars = input
    val rs =s"""
       |${consume(ctx, null)}
     """.stripMargin
    LogUtil.doLog("＝＝＝＝ProjectExecMock　doConsume　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝结束＝＝＝＝＝＝＝＝＝＝",this.getClass)

    rs
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def children: Seq[SparkPlanMock] = child :: Nil
}
