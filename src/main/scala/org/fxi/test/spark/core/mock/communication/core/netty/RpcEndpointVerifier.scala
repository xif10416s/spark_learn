package org.fxi.test.spark.core.mock.communication.core.netty

import org.fxi.test.spark.core.mock.communication.core.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
  * 终端验证，是否存在，并且可以连接成功
  * 通常注册的时候会先检查一次，保证远端有对应的endpoint能接收并处理该消息
  * @param rpcEnv
  * @param dispatcher
  */
class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => {
      val result = dispatcher.verify(name)
      println(s"---------------------响应RpcEndpointVerifier的CheckExistence消息，检查endpoint-verifier是否存在。。。$name : $result")

      context.reply(result)
    }
  }
}

object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an `RpcEndpoint` exists. */
  case class CheckExistence(name: String)
}
