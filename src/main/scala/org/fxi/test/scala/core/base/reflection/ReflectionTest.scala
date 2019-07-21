package org.fxi.test.scala.core.base.reflection


import scala.language.experimental.macros
import scala.reflect.macros.Context
/**
  * Created by xifei on 16-8-23.
  */
object ReflectionTest {

  import scala.reflect.runtime.{universe => ru}

  def main(args: Array[String]) {
    testBase()
    testMethodMirror()
    testFieldMirror()
    testClassMirror()
    testModuleMirror()
    testCompileTimeMirrors()
  }

  def testBase() = {
    val l = List(1, 2, 3)
    def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
    val theType = getTypeTag(l).tpe
    val decls = theType.declarations.take(10)
    println(decls)
  }

  def testMethodMirror(): Unit = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(new C)
    println(im)
    println(im.instance.asInstanceOf[C].x)

    val methodX = ru.typeOf[C].declaration(ru.newTermName("x")).asMethod
    println(methodX)
    val mm = im.reflectMethod(methodX)
    println(mm)
    println(mm())
  }

  def testFieldMirror() = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(new D)
    val fieldX = ru.typeOf[D].declaration(ru.newTermName("x")).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    println(fmX.get)
  }

  def testClassMirror(): Unit = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classE = ru.typeOf[E].typeSymbol.asClass
    val cm = m.reflectClass(classE)
    val ctorC = ru.typeOf[E].declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctorC)
    println(ctorm(2))
  }

  def testModuleMirror() = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val objectC = ru.typeOf[C.type].termSymbol.asModule
    val mm = m.reflectModule(objectC)
    val obj = mm.instance
    println(obj)
  }

  def testCompileTimeMirrors() = {
    println(Location("1",1,1))
  }
}

class C {
  def x = 2
}

class D {
  val x = 2;
  var y = 3
}

case class E(x: Int)

object C {
  def x = 2
}


case class Location(filename: String, line: Int, column: Int)