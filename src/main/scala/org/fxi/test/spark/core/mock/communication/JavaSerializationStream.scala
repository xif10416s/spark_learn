package org.fxi.test.spark.core.mock.communication

import java.io.{NotSerializableException, ObjectOutputStream, OutputStream}

import org.apache.spark.serializer.{SerializationDebugger, SerializationStream}

import scala.reflect.ClassTag

class JavaSerializationStream(out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
  extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  /**
    * Calling reset to avoid memory leak:
    * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
    * But only call it every 100th time to avoid bloated serialization streams (when
    * the stream 'resets' object class descriptions have to be re-written)
    */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw e
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}