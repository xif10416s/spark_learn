package org.fxi.test.scala.core.base.reflection

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, _}


/**
  * Created by xifei on 16-8-15.
  * ClassTag 只保存类型信息
  * TypeTag 类型 + 参数类型
  *
  * Scala2.8引入了一种串联和访问隐式参数的快捷方式。
  * def foo[A](implicit x: Ordered[A]) {}
  *
  * def foo[A : Ordered] {}
  *
  * [T: TypeTag] ==
  *  the compiler will simply generate an implicit parameter of type TypeTag[T]
  *
  *  TypeTag ==> 具体的类型
  *  WeakTypeTags ==> 任意类型
  */
class TestClassTag {

}

object TestClassTag {
  def main(args: Array[String]) {
    def mkArray[T: ClassTag](elems: T*) = Array[T](elems: _*)
    mkArray(1, 2)
    mkArray(List(1, 2), List(1, 2)).apply(0)
    mkArray(List(1, 2), List("1", "2")).apply(0)
    typeTag[List[String]]
    classTag[List[String]]
    typeOf[List[String]]
    classOf[List[String]]

    def paramInfo[T:ClassTag](x:T) : Unit = {
        println(classTag[T].runtimeClass)
    }

    def paramInfo20[T](x:T) : Unit = {
//      println(typeTag[T].tpe)
    }

    def paramInfo2[T:TypeTag](x:T) : Unit = {
      println(typeTag[T].tpe)
    }

    def paramInfo3[T: TypeTag](x: T): Unit = {
      val targs = typeOf[T] match { case TypeRef(_, _, args) => args }
      println(s"type of $x has type arguments $targs")
    }

    def paramInfo4[T: WeakTypeTag](x: T): Unit = {
      val targs = weakTypeTag[T] match { case TypeRef(_, _, args) => args }
      println(s"type of $x has type arguments $targs")
    }

    def foo[T] = paramInfo4(List[T]())

    def meth[A : TypeTag](xs: List[A]) = typeOf[A] match {
      case t if t =:= typeOf[String] => "list of strings"
      case t if t <:< typeOf[Foo] => "list of foos"
    }
    import scala.reflect.runtime.{universe => ru}
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classPerson = ru.typeOf[Person].typeSymbol.asClass
    val cm = m.reflectClass(classPerson)
    val ctor = ru.typeOf[Person].declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctor)
    val p = ctorm("Mike")
  }
}
class Foo
case class Person(name: String)
