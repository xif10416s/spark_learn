package org.fxi.test.scala.core.collection

import scala.reflect.ClassTag

///**
//  * Created by xifei on 16-9-19.
//  */
//object ArrayTest {
//  def main(args: Array[String]) {
//      test()
//  }
//
//  def test() = {
//    val t = Test[String](ResolveRelations,ResolveReferences)
//    val curPlan:String = "curlplan"
//    val rs =  t.rules.foldLeft(curPlan) {
//      case (plan:String,rule:Rule[String]) =>
//        rule(plan)
//    }
//    println(rs)
//  }
//}
//
//case class Test[T](rules : Rule[T]*)
//abstract class Rule[T] {
//
//  /** Name for this rule, automatically inferred based on class name. */
//  val ruleName: String = {
//    val className = getClass.getName
//    if (className endsWith "$") className.dropRight(1) else className
//  }
//
//  def apply(plan: T): T
//}
//
//object ResolveRelations extends Rule[String] {
//  override def apply(plan: String): String = s"$plan => ResolveRelations"
//}
//
//
//object ResolveReferences extends Rule[String] {
//  override def apply(plan: String): String = s"$plan => ResolveReferences"
//}
