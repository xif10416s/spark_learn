package org.fxi.test.scala.core.base.bean

/**
  * Created by xifei on 16-8-29.
  */
sealed abstract class Expr {

}

case class Var(name: String) extends Expr
case class Number(num: Double) extends Expr
case class UnOp(operator: String, arg: Expr) extends Expr
case class BinOp(operator: String,
                 left: Expr, right: Expr) extends Expr

//case class Var2() extends Expr
//
//case class Var3() extends Var2