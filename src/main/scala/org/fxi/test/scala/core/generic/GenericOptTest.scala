package org.fxi.test.scala.core.generic

import scala.reflect.ClassTag

/**
  * Created by xifei on 16-8-16.
  */
object GenericOptTest {
  def main(args: Array[String]) {
    doVariantTest()
  }

  /**
    * 0.子类可以赋值给父类 -- val a:Animal = new Dog()
    * 1.Animal 是 Dog 父类
    * 2.带类型参数的 ==> Invariant[Animal] 和 Invariant[Dog] 没有关系
    * 3.[+A]协变 =>B =  A 或 A的父类  ---  Covariant[B] 是 Covariant[A] 的父类
    * 4.[-A]逆变 =>B =   A 或 A的子类 ---  Covariant[B] 是 Covariant[A] 的父类
    *
    * sample:
    * trait Function1 [-T1, +R] extends AnyRef
    * 输入参数 -T 逆变 , 返回 R 协变
    * 输入 T的子类,返回R的父类
    */
  def doVariantTest(): Unit = {
    val cv: Covariant[Any] = new Covariant[Animal]
    val cv1: Covariant[Animal] = new Covariant[Animal]
    val cv2: Covariant[Dog] = new Covariant[BigDog]
    //    val cv2: Covariant[Dog] = new Covariant[Animal]

    val cvi: Contravariant[Dog] = new Contravariant[Animal]
    val cvi1: Contravariant[Animal] = new Contravariant[Animal]
    val cvi2: Contravariant[BigDog] = new Contravariant[Animal]
    //    val cvi1: Contravariant[Any] = new Contravariant[Animal]

    val ci: Invariant[Animal] = new Invariant[Animal]
    //    val ci1: Invariant[Any] = new Invariant[Animal]

    //                       =  container =  function1(-T,+R)
    // T = Animal , R=BigDog
    // -T = Dog , +R = Dog
    val getTweet: Dog => Dog = (a: Animal) => new BigDog()
    println(getTweet)
    println(getTweet(new BigDog()).name)


    // upper bound
    def test[T <: Animal](things:Seq[T]) = things.map(_.name)
    println(test(Seq(new Dog(),new BigDog())))
  }

}

// A或A的父类
class Covariant[+A]

//A 或A的子类
class Contravariant[-A]

class Invariant[A]

class Animal(val name: String = "animal")

class Dog(override val name: String = "dog") extends Animal

class BigDog(override val name: String = "BigDog") extends Dog