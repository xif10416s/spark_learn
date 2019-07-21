package org.fxi.test.spark.sql.catalyst.codegenerator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodeAndComment, CodeFormatter}
import org.apache.spark.sql.execution.BufferedRowIterator

/**
  * Created by xifei on 16-9-30.
  */
object CodeGeneratorTest {
  def main(args: Array[String]) {
    val cleanedSource = doCodeGen()
    println(cleanedSource.body)
    val clazz = CodeGenerator.compile(cleanedSource)
    println(clazz.getClass)

    val buffer =clazz._1.generate(Array(1,2,3,4)).asInstanceOf[BufferedRowIterator]
    buffer.init(1,Array(Iterator(InternalRow.empty,InternalRow.empty,InternalRow.empty)))
    buffer.hasNext()
  }

  def doCodeGen(): CodeAndComment = {
    val source =
      s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        private scala.collection.Iterator scan_input;

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
          partitionIndex = index;
          scan_input = inputs[0];
        }

        protected void processNext() throws java.io.IOException {
          while (scan_input.hasNext()) {
             System.out.println(scan_input.next());
          }
        }
      }
      """.trim
    new CodeAndComment(CodeFormatter.stripExtraNewLines(source), Map())
  }
}
