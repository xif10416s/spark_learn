package org.fxi.test.scala.syntax

import org.scalatest.FunSuite
class BaseTester extends FunSuite{


  def doProduce(a:String):String = {
    "123"
  }

  test("多行拼接测试") {
    val a = "11"
    val rs =s"""
               |${a}
               |${doProduce(s"${a}")}
     """.stripMargin
    println(rs)
  }
}
