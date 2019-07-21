package org.fxi.test.scala.core.function

/**
  * Created by xifei on 16-9-7.
  * 偏函数是只对函数定义域的一个子集进行定义的函数。 scala中用scala.PartialFunction[-T, +S]类来表示
  * 函数　y = f(x) ,每个ｘ都有对应的ｙ值
  * 偏函数　ｘ不一定有对应的ｙ值
  *
  * -------
  *
  * 部分应用函数, 是指一个函数有N个参数, 而我们为其提供少于N个参数, 那就得到了一个部分应用函数.
  */
object PartialFunctionTest {
  def main(args: Array[String]) {

    val pf: PartialFunction[Int, String] = {
      case i if i%2 == 0 => "even"
    }

    val tf: (Int => String) = pf orElse { case _ => "odd"}

    val sample = 1 to 10

    println(sample.collect(pf))

    val publisher:Publisher[Int] = new Publisher[Int] {
      override def subscribe(f: PartialFunction[Int, Int]): Unit = {
        println(f(1))
      }
    }
    publisher.subscribe({
      case i => i+1
    })


    def p1:PartialFunction[Int, Int] = {
      case x if x > 1 => 1
    }

    def p2 = (x:Int) => x match {
      case x if x > 1 => 1
    }
  }
}


trait Publisher[T] {
  def subscribe(f: PartialFunction[T, T])
}