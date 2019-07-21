package org.fxi.test.scala.core.base



/**
  * Created by xifei on 16-4-25.
  */
class CompanionTest  {
  def doTest(x:String) : Int = {
    println(CompanionTest.a)
    CompanionTest.test(1).toInt
  }
}

object CompanionTest {
  val a = "a";

  def test(x: Int): String = {
    println(x)
    x.toString
  }

  def main(args: Array[String]) {
    new CompanionTest().doTest("1")
  }
}
