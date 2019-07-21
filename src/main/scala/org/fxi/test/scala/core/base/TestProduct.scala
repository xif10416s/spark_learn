package org.fxi.test.scala.core.base

/**
  * Created by xifei on 16-8-15.
  * 提供了统一的方式(索引)访问对象
  */
class TestProduct(val a: String, val b: Int) extends Product {
  override def productElement(n: Int): Any = n match {
    case 0 => a
    case 1 => b
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  override def productArity: Int = 2

  override def canEqual(that: Any): Boolean = true

  def iter() ={
    println(productIterator.size)
    productIterator
    productIterator.map( {
      case i:String => i+"==>ss"
      case k:Int => k+1
    }).foreach(println _)
  }

}

object TestProduct {
  def main(args: Array[String]) {
    val p = new TestProduct("1", 2)
    println(p.productElement(0))
    println(p.productElement(1))

    p.iter()

    val p2 = new TestProduct4("1",1,"2",3L)
    println(p2._1)
    println(p2.productElement(0))
  }
}

class TestProduct4( a: String,  b: Int,  c: String,  d: Long) extends Product4[String,Int,String,Long] {
  override def _1: String = a

  override def _3: String = c

  override def _2: Int = b

  override def _4: Long = d

  override def canEqual(that: Any): Boolean = true
}
