package org.fxi.test.spark.mllib


import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.fxi.test.spark.common.constants.CommonConstants

/**
  * Created by xifei on 16-5-18.
  */
case class KeyedPoint(label: String,
                      features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }
}

object KeyedPoint {
  def parse(s: String): KeyedPoint = {

    val parts = s.split(CommonConstants.COMON)
    val label = parts(0)
    val features = Vectors.dense(parts(1).trim().split(CommonConstants.TAB).map(java.lang.Double.parseDouble))
    new KeyedPoint(label, features)
  }
}
