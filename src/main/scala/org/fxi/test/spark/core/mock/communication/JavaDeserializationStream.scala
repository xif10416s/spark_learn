package org.fxi.test.spark.core.mock.communication

import java.io.{InputStream, ObjectInputStream, ObjectStreamClass}

import org.apache.spark.serializer.DeserializationStream

import scala.reflect.ClassTag


class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
  extends DeserializationStream {
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Void]
  )

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]

  def close() {
    objIn.close()
  }
}
