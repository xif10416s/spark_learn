package org.fxi.test.spark.core.mock.communication.test.network

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, ObjectInputStream}
import java.nio.ByteBuffer

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.util.ByteBufferInputStream
import org.fxi.test.spark.core.mock.communication.{ByteBufferInputStream, JavaDeserializationStream}
import org.fxi.test.spark.core.mock.communication.network.client.{RpcResponseCallback, TransportClient}
import org.fxi.test.spark.core.mock.communication.network.server.RpcHandler


/**
  * 服务端和客户端用了同一个rpchandler实现
  */
class TestRpcHandler(val role:String) extends RpcHandler {
  override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
    val bis = new ByteBufferInputStream(message)
    val in = new DataInputStream(bis)

    val str = in.readUTF()
    println(s"receive from  : " +  client.getSocketAddress + "    " + str )
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    out.writeUTF(s"hi from : $role ")
    val buffer = ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
//    client.sendRpc(buffer,callback)
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
