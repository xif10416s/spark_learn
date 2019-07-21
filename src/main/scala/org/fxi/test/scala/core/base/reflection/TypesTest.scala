package org.fxi.test.scala.core.base.reflection

import scala.reflect.api.JavaUniverse

/**
  * Created by xifei on 16-8-23.
  */
object TypesTest {
  def main(args: Array[String]) {
    import scala.reflect.runtime.universe._
    val unit: JavaUniverse#Type = typeOf[List[Int]]
    println(unit)

    println( typeOf[Parent] <:< typeOf[Child])
    println( typeOf[Child] <:< typeOf[Parent])

    println(typeOf[Double] weak_<:< typeOf[Int])

    println(typeOf[Int] <:< typeOf[Double])
    println(typeOf[Int] weak_<:< typeOf[Double])

    val p1= new Parent()
    val p2 =new Parent()
    def getType[T: TypeTag](obj: T) = typeOf[T]

    println(getType(p1) =:= getType(p2))
    println(getType(p1).members)
    println(getType(p1).declarations)

    println(typeOf[List[_]].member("map": TermName))

    println(typeOf[List[_]].member("S1elf": TypeName))

    typeOf[List[Int]].members.filter(_.isPrivate).foreach(println _)
  }
}


class Parent
class Child extends Parent