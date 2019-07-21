package org.fxi.test.scala.core.machinelearing.breeze

import breeze.linalg._
import breeze.plot._
import org.scalatest.FunSuite

/**
  * Created by xifei on 16-6-17.
  */
class BreezeTest extends FunSuite{
  test("基础作图")  {
    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(-5.0, 5.0)
    p.ylim(-1.0,3.0)
//    p += plot(x, x :^ 2.0)
//    p += plot(x, x :^ 3.0, '.')
//    p += plot(x, x :^ 4.0, '+')
    p += plot(x, 1.0 /:/ (1.0 :+ 1.0 ^:^  -x), '-')
    p.xlabel = "x axis"
    p.ylabel = "y axis"
    val p2 = f.subplot(2,1,1)
    val g = breeze.stats.distributions.Gaussian(0,1)
    p2 += hist(g.sample(100000),100)
    p2.title = "A normal distribution"
//    f.saveas("subplots.png")

//    val f2 = Figure()
//    f2.subplot(0) += image(DenseMatrix.rand(200,200))
//    f2.saveas("image.png")
    Thread.sleep(100000)
  }


  test("group by") {
    val x = DenseVector.zeros[Double](5)
    x(3 to 4) := .5
    x(1)=2
    println(x)
    println(DenseVector(1,2,3))
    val m = DenseMatrix.zeros[Int](5,5)
    println(m)
    println(m(::,1))
    m(4,::) := DenseVector(1,2,3,4,5).t
    println(DenseVector(1,2,3,4,5))
    println(m)
    println(sum(m))
    println(m+m)
    println(m *:* m)
    println(m:+=1)
  }
}
