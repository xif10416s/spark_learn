package org.fxi.test.spark.core.mock.communication.test.network

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.fxi.test.spark.core.mock.communication.ByteBufferInputStream
import org.fxi.test.spark.core.mock.communication.network.client.{RpcResponseCallback, TransportClient}
import org.fxi.test.spark.core.mock.communication.network.server.RpcHandler
import org.slf4j.LoggerFactory


/**
  * 服务端和客户端用了同一个rpchandler实现
  */
class TestRpcHandler(val role:String) extends RpcHandler {
  private val logger = LoggerFactory.getLogger(classOf[TestRpcHandler])

  override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
    logger.trace("TestRpcHandler on receive start .............")
    // 1 读取接收到的请求消息
    val bis = new ByteBufferInputStream(message)
    val in = new DataInputStream(bis)
    val str = in.readUTF()
    // 2 执行对应的业务逻辑处理
    logger.trace("TestRpcHandler do process .............")
    println(s"receive from  : " +  client.getSocketAddress + "    " + str )
    // 3 返回相应消息
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    out.writeUTF(s"hi from : $role ")
    val buffer = ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
//    client.sendRpc(buffer,callback)
    logger.trace("TestRpcHandler do callback call .............")
    callback.onSuccess(buffer)
    client.close()
  }

  private def readRpcAddress(in: DataInputStream): (_,_) = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      (in.readUTF(), in.readInt())
    } else {
      null
    }
  }
}
