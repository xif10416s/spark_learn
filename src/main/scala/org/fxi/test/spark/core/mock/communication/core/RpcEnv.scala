package org.fxi.test.spark.core.mock.communication.core

import java.nio.channels.ReadableByteChannel
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.fxi.test.spark.core.mock.communication.core.netty.NettyRpcEnvFactory

import scala.concurrent.Future
import scala.concurrent.duration.Duration
object RpcEnv {

  def create(
              name: String,
              host: String,
              port: Int,
              conf: SparkConf,
              clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, 0, clientMode)
  }

  def create(
              name: String,
              bindAddress: String,
              advertiseAddress: String,
              port: Int,
              conf: SparkConf,
              numUsableCores: Int,
              clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port,
      numUsableCores, clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}

abstract class RpcEnv(conf: SparkConf) {
  /**
    * 根据endpoint 获取指定的endpointRef
    */
  def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Return the address that [[RpcEnv]] is listening to.
    */
  def address: RpcAddress

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
    */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
     asyncSetupEndpointRefByURI(uri).result(Duration.create(3L,TimeUnit.SECONDS))(null.asInstanceOf[scala.concurrent.CanAwait])
  }

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
    * This is a blocking action.
    */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  def shutdown(): Unit

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    */
  def awaitTermination(): Unit

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  def deserialize[T](deserializationAction: () => T): T



  /**
    * Open a channel to download a file from the given URI. If the URIs returned by the
    * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
    * retrieve the files.
    *
    * @param uri URI with location of the file.
    */
  def openChannel(uri: String): ReadableByteChannel
}

case class RpcEnvConfig(conf: SparkConf,
                                        name: String,
                                        bindAddress: String,
                                        advertiseAddress: String,
                                        port: Int,
                                        numUsableCores: Int,
                                        clientMode: Boolean)

