package org.fxi.test.spark.core.mock.communication.core

import org.apache.spark.SparkException

/**
  * 监听一些事件的生命周期，如start,stop
  * 主要定义了一些接收到消息的处理接口
  */
trait RpcEndpoint {
  /**
    * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
    */
  val rpcEnv: RpcEnv

  /**
    * 返回当前endpoint的 endpointRef , 当执行完onStart方法后，self才初始化，并且被注册到RpcEnv中
    */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
    * RpcEndpointRef.send 或者 RpcCallContext.reply 发送消息后，触发Endpoint的receive方法
    *
    */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
    *  处理RpcEndpointRef.ask 发送的消息
    */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
    * Invoked when any exception is thrown during handling messages.
    */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
    * Invoked when `remoteAddress` is connected to the current node.
    */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when `remoteAddress` is lost.
    */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when some network error happens in the connection between the current node and
    * `remoteAddress`.
    */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked before [[RpcEndpoint]] starts to handle any message.
    */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
    * use it to send or ask messages.
    */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
    * A convenient method to stop [[RpcEndpoint]].
    */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      //rpcEnv.stop(_self)
    }
  }
}

/**
  * A trait that requires RpcEnv thread-safely sending messages to it.
  *
  * Thread-safety means processing of one message happens before processing of the next message by
  * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
  * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
  * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
  *
  * However, there is no guarantee that the same thread will be executing the same
  * [[ThreadSafeRpcEndpoint]] for different messages.
  */
trait ThreadSafeRpcEndpoint extends RpcEndpoint