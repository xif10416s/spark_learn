package org.fxi.test.scala.core.base.reflection

/**
  * Created by xifei on 16-8-23.
  */
object TreesTest {

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}
  import scala.language.experimental.macros

  def main(args: Array[String]) {
    val tree = Apply(Select(Ident(ru.newTermName("x")), ru.newTermName("$plus")), List(Literal(Constant(2))))
    println(showRaw(tree))
    println(tree)
    show(tree)
    val (fun, arg) = tree match {
        case Apply(fn, a :: Nil) => (fn, a)
    }
    println(fun)
    println(arg)

    val Apply(fun1, arg1 :: Nil) = tree
    //creates an AST representing a literal 5 in Scala source code:
    println(Literal(Constant(5)))
    //creates an AST representing `print("Hello World")`:
    val pr = Apply(Select(Select(This(newTypeName("scala")), newTermName("Predef")), newTermName("print")), List(Literal(Constant("Hello World"))))


    // creates an AST from a literal 5, and then uses `showRaw` to print it in a readable format.
    println(showRaw(reify {
      println("aa")
    }.tree))
    println(reify {
      println("aa")
    }.tree)

    val expr = reify { class Flower { def name = "Rose" } }

    def test = macro Macros.impl
//    test


    val tree2 = reify { "test".length }.tree
    import scala.tools.reflect.ToolBox
    val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
    val ttree = tb.typeCheck(tree2)
    println(ttree)
    println(ttree.tpe)
    println(ttree.symbol)
    val compile: () => Any = tb.compile(tree2)
    compile
    println(tb.eval(tree2))


    import Macros._
//    printf("hello %s","www")
  }

}
