package org.fxi.test.spark.core.mock.communication.test.core

import org.apache.spark.SparkConf

import org.apache.spark.internal.Logging
import org.fxi.test.spark.core.mock.communication.core.{RpcEnv, ThreadSafeRpcEndpoint}
import org.slf4j.LoggerFactory

object TestMaster {
  val ENDPOINT_NAME = "Master"
  private val logger = LoggerFactory.getLogger("TestMaster")
  def main(args: Array[String]): Unit = {
    logger.trace("Master 初始化 NettyRpcEnv .....")
    val rpcEnv = RpcEnv.create("Master","localhost","localhost",7070,new SparkConf(),3,false)
    rpcEnv.setupEndpoint(ENDPOINT_NAME,new TestMaster(rpcEnv))

    rpcEnv.awaitTermination()
  }
}

class TestMaster(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {
  override def receive: PartialFunction[Any, Unit] = {
    case RegisterApplication(description, driver) =>
       {
        logInfo("Registering app " + description)
        // DO SOME THING REGISTER
        logInfo("do app register............ " )
        logInfo("send RegisteredApplication message to  driver")
        driver.send(RegisteredApplication("id123", self))
      }
  }
}
