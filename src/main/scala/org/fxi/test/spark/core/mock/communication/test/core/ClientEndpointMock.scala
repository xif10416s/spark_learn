package org.fxi.test.spark.core.mock.communication.test.core



import org.apache.spark.internal.Logging
import org.fxi.test.spark.core.mock.communication.core.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}


class ClientEndpointMock (override val rpcEnv: RpcEnv,masterHost:String,masterPort:Int) extends ThreadSafeRpcEndpoint
  with Logging {
  override def onStart(): Unit = {
    log.trace("ClientEndpointMock on start .......")
    log.trace(s"ClientEndpointMock setupEndpointRef  ....$masterHost , $masterPort ")
    val masterRef = rpcEnv.setupEndpointRef(RpcAddress(masterHost,masterPort), TestMaster.ENDPOINT_NAME)
    masterRef.send(RegisterApplication("this is test driver description", self))
  }

  override def receive: PartialFunction[Any, Unit] = {

      case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases?
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        log.info(s"receive from master RegisteredApplication appID [$appId_]")

    }
}
