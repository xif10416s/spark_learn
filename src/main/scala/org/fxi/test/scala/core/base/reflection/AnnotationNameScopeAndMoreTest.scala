package org.fxi.test.scala.core.base.reflection

/**
  * Created by xifei on 16-8-24.
  */
object AnnotationNameScopeAndMoreTest {

  import scala.reflect.runtime.universe._
  import scala.reflect.api._

  def main(args: Array[String]) {
    testName()
  }

  def testName(): Unit = {
    val mapName= newTermName("map")
    val listTpe = typeOf[List[Int]]
    listTpe.member(mapName)

  }
}
