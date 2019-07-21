package org.fxi.test.scala.syntax

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.language.experimental.macros
import scala.reflect.api.JavaUniverse

/**
  * Created by xifei on 16-8-18.
  * 1.构建抽象语法树，运行时编译生成字节吗
  * 2.use Catalyst to
  * transform a tree representing an expression in SQL to an AST for
  * Scala code to evaluate that expression, and then compile and run the
  * generated code.
  * 将ｓｑｌ中的表达式转换成ｓｃａｌａ中的ＡＳＴ，用来计算这个表达式，然后编译运行生成的代码
  */
object TestQuasiquotes {
  def main(args: Array[String]) {
    val tree1: JavaUniverse#Tree = q"i am { a quasiquote }"
    println(tree1)
    println(tree1 match { case q"i am { a quasiquote }" => "it worked!" })


    import scala.tools.reflect.ToolBox
    val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
    val a1 = 3
    val a = q"1 + ${a1}"

    showRaw(a)
    println(tb.eval(a))

    val src = "case class A(a:String) \n " +
      "val testa = A(\"a\") \n " +
      "println(testa.a)"
    val rs = tb.eval(q"$src")

    println(rs)
  }
}

case class A(a:String)