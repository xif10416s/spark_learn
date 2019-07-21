package org.fxi.test.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.{Literal, Substring}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String


/**
  * Created by xifei on 16-9-5.
  */
class TestExpression {

  def testSubString() = {
    val sb = Substring(Literal(UTF8String.fromString("abc"),StringType),Literal(1,IntegerType),Literal(2,IntegerType))
    println( sb.eval())
  }
}
