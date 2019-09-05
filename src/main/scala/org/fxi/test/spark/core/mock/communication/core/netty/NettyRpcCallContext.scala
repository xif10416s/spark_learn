/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fxi.test.spark.core.mock.communication.core.netty

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.netty.RpcFailure
import org.fxi.test.spark.core.mock.communication.core.{RpcAddress, RpcCallContext}
import org.fxi.test.spark.core.mock.communication.network.client.RpcResponseCallback

import scala.concurrent.Promise

/**
  * Rpc调用上下文封装，分为两类：远程调用 和 本地调用
  * @param senderAddress
  */
abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext with Logging {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }

}

/**
  * 如果发送消息和接收消息是在一个进程中，通过Promise，响应 send的消息
 * If the sender and the receiver are in the same process, the reply can be sent back via `Promise`.*
 */
private[netty] class LocalNettyRpcCallContext(
    senderAddress: RpcAddress,
    p: Promise[Any])
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

/**
  * 远程地址调用通过RpcResponseCallback回调函数响应消息
 */
class RemoteNettyRpcCallContext(
    nettyEnv: NettyRpcEnv,
    callback: RpcResponseCallback,
    senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}
