package org.fxi.test.spark.core.mock.communication.test.network

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.lang
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Map

import org.apache.spark.network.util.{ConfigProvider, TransportConf}
import org.fxi.test.spark.core.mock.communication.ByteBufferInputStream
import org.fxi.test.spark.core.mock.communication.network.TransportContext
import org.fxi.test.spark.core.mock.communication.network.client.{RpcResponseCallback, TransportClient}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 模拟netty server
  * 参考NettyRpcEnv 简单实现
  */
object NetworkClientTest {
  private val logger = LoggerFactory.getLogger(classOf[TransportClient])

  // 初始化conf
    val conf = new TransportConf("Test_server_main", new ConfigProvider() {
        override def get(name: String): String = {
            ""
        }

        override def getAll: lang.Iterable[Map.Entry[String, String]] = {
            null
        }
    })

    // 初始化自定义rpcHandler
    val rpcHandler = new TestRpcHandler("client_role")

    def main(args: Array[String]): Unit = {
        // 初始化Context
        val context = new TransportContext(conf,rpcHandler)
        // 当前主机服务
        val address = new InetSocketAddress(8787)
        val client = context.createClient(address,"test")
        //  spark 中发送的消息 有一定的结构 参考 ：RequestMessage
        val bos = new ByteArrayOutputStream()
        val out = new DataOutputStream(bos)
        out.writeUTF("hello server")
        val buffer = ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
        client.sendRpc(buffer,new RpcResponseCallback(){
            /**
              * Successful serialized result from server.
              *
              * rpc成功回调函数
              */
            override def onSuccess(response: ByteBuffer): Unit = {
                logger.trace("RpcResponseCallback call onSuccess  start.......")
                val bis = new ByteBufferInputStream(response)
                val in = new DataInputStream(bis)

                val str = in.readUTF()
                println(s"receive from  : " +  client.getSocketAddress + "    " + str )
                logger.trace(" RpcResponseCallback call server onSuccess finish....")
            }

            /** rpc异常回调函数 */
            override def onFailure(e: Throwable): Unit = {
                println("--------------------- call server onFailure ....")
            }
        })
        Thread.sleep(30000)
    }
}
