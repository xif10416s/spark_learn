package org.fxi.test.scala.core.base

import org.fxi.test.scala.core.base.bean._


/**
  * Created by xifei on 16-8-29.
  */
class PatternTest {

  def testConstructor(): Unit = {
    def simplifyTop(expr: Expr): Expr = expr match {
      case UnOp("-", UnOp("-", e))
      =>
        println(e)
        e
      // Double negation
      case BinOp("+", e, Number(0)) =>
        println(e)
        e // Adding zero
      case BinOp("*", e, Number(1)) =>
        println(e)
        e
      case BinOp(_, _, _) => println(expr + " is a binary operation")
        expr
      case _ => expr // Multiplying by one
    }
    simplifyTop(new UnOp("-", new UnOp("-", new Number(1))))
    simplifyTop(new BinOp("+", new Number(1), new Number(0)))
    simplifyTop(new BinOp("3", new Number(1), new Number(0)))
  }


  def testSequencepatterns(): Unit = {
    def simplifyTop(expr: List[Int]): Unit = expr match {
      case List(0, _*) => println("found it")
      case _ =>
    }
    simplifyTop(List(0, 1));
    simplifyTop(List(1, 1));
  }



  def testTuplepatterns(): Unit = {
    def tupleDemo(expr: Any) =
      expr match {
        case (a, b, c)
        =>
          println("matched " + a + b + c)
        case _ =>
      }
    tupleDemo(("a ", 3, "-tuple"))
  }



  def testTypedpatterns(): Unit = {
    def generalSize(x: Any) = x match {
      case s: String => println(s.length)
      case m: Map[_, _] => println(m.size)
      case _ => -1
    }
    generalSize("abc")
    generalSize(Map(1 -> 'a', 2 -> 'b'))


    def isIntIntMap(x: Any) = x match {
      case m: Map[Int, Int] => println(true) //erasure model of generics  //expect Array
      case _ => println(false)
    }
    isIntIntMap(Map("abc" -> "abc"))

  }

  //You simply write the variable name, an at sign ( @ ), and
  // bind name

  def testVariablepatterns(): Unit = {
    def test(expr: Expr) = expr match {
      case UnOp(a@"abs", e1@UnOp("abs", _)) => println(a+" "+e1)
      case _ =>
    }

    test(new UnOp("abs",new UnOp("abs", new Number(1))))
  }


  def testguardspatterns(): Unit = {
    def simplifyAdd(e: Expr) = e match {
      case BinOp("+", x, y) if x == y =>
        BinOp("*", x, Number(2))
      case _ => e
    }

    // match only positive integers
    //case n: Int if 0 < n => ...
    // match only strings starting with the letter ‘a’
    //case s: String if s(0) == 'a' => ...
  }

  /**
    * A sealed class cannot have any new subclasses added except the ones in the
same file
    */

  def testSealedClasses():Unit ={
    def describe(e: Expr): Unit = (e: @unchecked) match {
      case Number(_) => println("a number ----------" + e)
      case Var(_)
      => println("a variable")
    }
    describe(new Number(1))
  }
}
