package org.fxi.test.spark.core.mock.communication.core

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * 主要是给这个终端发送消息的方法
  * @param conf
  */
abstract  class RpcEndpointRef(conf: SparkConf) extends Serializable with Logging {

  val defaultAskTimeout =  RpcTimeout(conf,"")
  /**
    * 终端的地址，主机 + port
    */
  def address: RpcAddress

  def name: String

  /**
    * Sends a one-way asynchronous message. Fire-and-forget semantics.
    */
  def send(message: Any): Unit

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within the specified timeout.
    *
    * This method only sends the message once and never retries.
    */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within a default timeout.
    *
    * This method only sends the message once and never retries.
    */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
    * default timeout, throw an exception if this fails.
    *
    * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
    * loop of [[RpcEndpoint]].

    * @param message the message to send
    * @tparam T type of the reply message
    * @return the reply message from the corresponding [[RpcEndpoint]]
    */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
    * specified timeout, throw an exception if this fails.
    *
    * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
    * loop of [[RpcEndpoint]].
    *
    * @param message the message to send
    * @param timeout the timeout duration
    * @tparam T type of the reply message
    * @return the reply message from the corresponding [[RpcEndpoint]]
    */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }
}
