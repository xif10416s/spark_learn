package org.fxi.test.scala.core.base.reflection

import scala.language.experimental.macros
import scala.reflect.macros.Context
import scala.collection.mutable.{ListBuffer, Stack}

/**
  * Created by xifei on 16-8-23.
  */

object Macros {
  //  def currentLocation: Location = macro impl
  //
  //  def impl(c: Context): c.Expr[Location] = {
  //    import c.universe._
  //    val pos = c.macroApplication.pos
  //    val clsLocation = c.mirror.staticModule("Location") // get symbol of "Location" object
  //    println(pos.source.path)
  //    println(pos.line)
  //    c.Expr(Apply(Ident(clsLocation), List(Literal(Constant(pos.source.path)), Literal(Constant(pos.line)), Literal(Constant(pos.column)))))
  //  }

  def impl(c: scala.reflect.macros.Context) = c.Expr[Unit](c.parse("println(2+3)"))

  def printf(format: String, params: Any*): Unit = macro printf_impl

  def printf_impl(c: Context)(format: c.Expr[String], params: c.Expr[Any]*): c.Expr[Unit] = {
    import c.universe._
    val Literal(Constant(s_format: String)) = format.tree
    val evals = ListBuffer[ValDef]()
    def precompute(value: Tree, tpe: Type): Ident = {
      val freshName = newTermName(c.fresh("eval$"))
      evals += ValDef(Modifiers(), freshName, TypeTree(tpe), value)
      Ident(freshName)
    }
    val paramsStack = Stack[Tree]((params map (_.tree)): _*)
    val refs = s_format.split("(?<=%[\\w%])|(?=%[\\w%])") map {
      case "%d" => precompute(paramsStack.pop, typeOf[Int])
      case "%s" => precompute(paramsStack.pop, typeOf[String])
      case "%%" => Literal(Constant("%"))
      case part => Literal(Constant(part))
    }
    val stats = evals ++ refs.map(ref => reify(print(c.Expr[Any](ref).splice)).tree)
    c.Expr[Unit](Block(stats.toList, Literal(Constant(()))))
  }
}
