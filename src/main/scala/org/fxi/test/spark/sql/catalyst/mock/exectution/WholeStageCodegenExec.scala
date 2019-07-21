package org.fxi.test.spark.sql.catalyst.mock.exectution


import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BoundReference, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.fxi.test.spark.sql.catalyst.mock.{Driver, LogUtil, SparkPlanMock}

/**
  * Created by xifei on 16-10-17.
  */
trait CodegenSupportMock extends SparkPlanMock {

  /** Prefix used in the current operator's variable names. */
  private def variablePrefix: String = this match {
    case _ => nodeName.toLowerCase
  }


  /**
    * Whether this SparkPlan support whole stage codegen or not.
    */
  def supportCodegen: Boolean = true

  /**
    * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
    */
  protected var parent: CodegenSupportMock = null

  /**
    * Returns all the RDDs of InternalRow which generates the input rows.
    *
    * Note: right now we support up to two RDDs.
    */
  def inputRDDs(): Seq[RDD[InternalRow]]

  /**
    * Returns Java source code to process the rows from input RDD.
    */
  final def produce(ctx: CodegenContext, parent: CodegenSupportMock): String =  {
    LogUtil.doLog("******************************CodegenSupportMock produce  start*****************************",this.getClass)
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    val rs =s"""
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString}")}
       |${doProduce(ctx)}
     """.stripMargin
    LogUtil.doLog("******************************CodegenSupportMock produce  end*****************************",this.getClass)
    rs
  }

  /**
    * Generate the Java source code to process, should be overridden by subclass to support codegen.
    *
    * doProduce() usually generate the framework, for example, aggregation could generate this:
    *
    *   if (!initialized) {
    *     # create a hash map, then build the aggregation hash map
    *     # call child.produce()
    *     initialized = true;
    *   }
    *   while (hashmap.hasNext()) {
    *     row = hashmap.next();
    *     # build the aggregation results
    *     # create variables for results
    *     # call consume(), which will call parent.doConsume()
    *      if (shouldStop()) return;
    *   }
    */
  protected def doProduce(ctx: CodegenContext): String

  /**
    * Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
    */
  final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
    LogUtil.doLog("******************************CodegenSupportMock consume  start*****************************",this.getClass)
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString}")}
       |${parent.doConsume(ctx,Nil,null)}
     """.stripMargin
    LogUtil.doLog("******************************CodegenSupportMock consume  end*****************************",this.getClass)
    ""
  }

  /**
    * Returns source code to evaluate all the variables, and clear the code of them, to prevent
    * them to be evaluated twice.
    */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code != "").map(_.code).mkString("\n")
//    variables.foreach(_.code = "")
    evaluate
  }

  /**
    * Returns source code to evaluate the variables for required attributes, and clear the code
    * of evaluated variables, to prevent them to be evaluated twice.
    */
  protected def evaluateRequiredVariables(
                                           attributes: Seq[Attribute],
                                           variables: Seq[ExprCode],
                                           required: AttributeSet): String = {
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code != "" && required.contains(attributes(i))) {
        evaluateVars.append(ev.code + "\n")
//        ev.code = ""
      }
    }
    evaluateVars.toString()
  }

  /**
    * The subset of inputSet those should be evaluated before this plan.
    *
    * We will use this to insert some code to access those columns that are actually used by current
    * plan before calling doConsume().
    */
  def usedInputs: AttributeSet = references

  /**
    * Generate the Java source code to process the rows from child SparkPlan.
    *
    * This should be override by subclass to support codegen.
    *
    * For example, Filter will generate the code like this:
    *
    *   # code to evaluate the predicate expression, result is isNull1 and value2
    *   if (isNull1 || !value2) continue;
    *   # call consume(), which will call parent.doConsume()
    *
    * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
    */
  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw new UnsupportedOperationException
  }
}


case class WholeStageCodegenExecMock (child: SparkPlanMock) extends SparkPlanMock with CodegenSupportMock {
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    LogUtil.doLog("＝＝＝＝WholeStageCodegenExecMock　doCodeGen　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)

    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupportMock].produce(ctx, this)
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      ${ctx.registerComment(s"""Codegend pipeline for\n${child.treeString.trim}""")}
      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
          partitionIndex = index;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source), ctx.getPlaceHolderToComments()))
    LogUtil.doLog("＝＝＝＝WholeStageCodegenExecMock　doCodeGen　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝结束＝＝＝＝＝＝＝＝＝＝",this.getClass)

    (ctx, cleanedSource)
  }

  override def doExecute(): RDD[InternalRow] = {
    LogUtil.doLog("＝＝＝＝WholeStageCodegenExecMock　doExecute　 开始全局生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)
    val (ctx, cleanedSource) = doCodeGen()
    // try to compile and fallback if it failed
    try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case e: Exception  =>
        // We should already saw the error message
        return child.execute()
    }
    val references = ctx.references.toArray


    LogUtil.doLog("＝＝＝＝WholeStageCodegenExecMock　doExecute　 开始全局生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝结束＝＝＝＝＝＝＝＝＝＝",this.getClass)
     val rows = Seq(new UnsafeRow(),new UnsafeRow(),new UnsafeRow(),new UnsafeRow())
    Driver.spark.sparkContext.makeRDD(rows,2).mapPartitionsWithIndex { (index, iter) =>
      println("1========doExecute mapPartitionsWithIndex   change iterator=======")
      val buffer = new BufferedRowIteratorMock()
      buffer.init(index, Array(iter))
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          println("3========doExecute mapPartitionsWithIndex   hasNext=======")
          val v = buffer.hasNext
          v
        }
        override def next: InternalRow = buffer.next()
      }
    }
//    Driver.spark.sparkContext.emptyRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    LogUtil.doLog("＝＝＝＝WholeStageCodegenExecMock　doConsume　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝开始＝＝＝＝＝＝＝＝＝＝",this.getClass)
    val doCopy = if (true) {
      ".copy()"
    } else {
      ""
    }
   val rs =  s"""
     """.stripMargin.trim
    LogUtil.doLog("＝＝＝＝WholeStageCodegenExecMock　doConsume　 生成执行代码＝＝＝＝＝＝＝＝＝＝＝＝end＝＝＝＝＝＝＝＝＝＝",this.getClass)
    rs
  }

//   def generateTreeString(
//                                   depth: Int,
//                                   lastChildren: Seq[Boolean],
//                                   builder: StringBuilder,
//                                   verbose: Boolean,
//                                   prefix: String = ""): StringBuilder = {
//    child.generateTreeString(depth, lastChildren, builder, verbose, "*")
//  }


  override def children: Seq[SparkPlanMock] = child :: Nil

  override def output: Seq[Attribute] = Nil
}

 class BufferedRowIteratorMock() extends BufferedRowIterator {
   var scan_input:Iterator[InternalRow] = null ;
  override def init(index: Int, iters: Array[Iterator[InternalRow]]): Unit = {
    scan_input = iters(0)
  }

  override def processNext(): Unit = {
    System.err.println("=============BufferedRowIteratorMock processNext 生成代码处理===")
    while (scan_input.hasNext) {
      println(scan_input.next().hashCode())
    }
  }
}
