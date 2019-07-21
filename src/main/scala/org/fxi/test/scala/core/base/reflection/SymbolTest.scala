package org.fxi.test.scala.core.base.reflection

import scala.reflect.api.JavaUniverse

/**
  * Created by xifei on 16-8-23.
  */
object SymbolTest {
  def main(args: Array[String]) {
    // conversion
    import scala.reflect.runtime.universe._
    import scala.reflect.runtime.{universe => ru}
    val member: JavaUniverse#Symbol = typeOf[C1[Int]].member(ru.newTermName("test"))
    val testMember = member
    val method: JavaUniverse#MethodSymbol = testMember.asMethod
  }
}
class C1[T] { def test[U](x: T)(y: U): Int = ??? }