package org.fxi.test.scala.core.base.bean

/**
  * Created by xifei on 16-8-30.
  */
class Operator {
  def transformExpressionsUp(rule:String):Int = {
    //do something
    println(rule)
    rule.hashCode
  }
}
