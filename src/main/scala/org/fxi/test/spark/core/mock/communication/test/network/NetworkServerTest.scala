package org.fxi.test.spark.core.mock.communication.test.network

import java.lang
import java.util.Map
import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ConfigProvider
import org.apache.spark.network.util.TransportConf
import org.fxi.test.spark.core.mock.communication.network.TransportContext
import org.fxi.test.spark.core.mock.communication.network.server.TransportServerBootstrap
import org.fxi.test.spark.core.mock.communication.util.ThreadUtils

/**
  * 模拟netty server
  * 参考NettyRpcEnv 简单实现
  */
object NetworkServerTest {

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
    val rpcHandler = new TestRpcHandler("server_role")

    def main(args: Array[String]): Unit = {
        // 初始化Context
        val context = new TransportContext(conf,rpcHandler)
        // 一些权限认证等预处理，此处省略
        val bootstraps: java.util.List[TransportServerBootstrap] = java.util.Collections.emptyList()
        val server = context.createServer(8787,bootstraps)

        ThreadUtils.newDaemonFixedThreadPool(1, "server").awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    }
}
