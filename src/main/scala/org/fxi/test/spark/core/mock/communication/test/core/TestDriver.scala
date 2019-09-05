package org.fxi.test.spark.core.mock.communication.test.core

import org.apache.spark.SparkConf
import org.fxi.test.spark.core.mock.communication.core.RpcEnv
import org.fxi.test.spark.core.mock.communication.test.core.TestMaster.logger
import org.slf4j.LoggerFactory

object TestDriver {
  private val logger = LoggerFactory.getLogger("TestDriver")
  def main(args: Array[String]): Unit = {
    logger.trace("Driver 初始化 NettyRpcEnv .....")
    val rpcEnv = RpcEnv.create("Master","localhost","localhost",7070,new SparkConf(),3,true)

    logger.trace("Driver 注册 ClientEndpointMock .....")
    val appClient = rpcEnv.setupEndpoint("AppClient", new ClientEndpointMock(rpcEnv,"localhost",7070))

    rpcEnv.awaitTermination()
  }
}
