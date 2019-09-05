package org.fxi.test.spark.core.mock.communication.core.netty

import java.io._
import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit, TimeoutException}

import javax.annotation.Nullable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.TransportClientBootstrap
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.StreamManager
import org.apache.spark.rpc.RpcEnvStoppedException
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.util.ThreadUtils
import org.fxi.test.spark.core.mock.communication.core._
import org.fxi.test.spark.core.mock.communication.network.TransportContext
import org.fxi.test.spark.core.mock.communication.network.client.{RpcResponseCallback, TransportClient}
import org.fxi.test.spark.core.mock.communication.network.server.{RpcHandler, TransportServer, TransportServerBootstrap}
import org.fxi.test.spark.core.mock.communication.util.ThreadUtils
import org.fxi.test.spark.core.mock.communication.{ByteBufferInputStream, ByteBufferOutputStream, JavaSerializer, JavaSerializerInstance}

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{DynamicVariable, Failure, Success}

/**
  *
  * @param conf
  * @param javaSerializerInstance
  * @param host  当前服务部署的主机地址
  * @param securityManager
  * @param numUsableCores
  */
class NettyRpcEnv(val conf: SparkConf,
                  javaSerializerInstance: JavaSerializerInstance,
                  host: String,
                  securityManager: SecurityManager,
                  numUsableCores: Int) extends RpcEnv(conf) with Logging {

  val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),
    "rpc",
    conf.getInt("spark.rpc.io.threads", numUsableCores))

  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)

  private val streamManager = null

  val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads", 2))

  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  // 一些初始化操作，此处省略，如权限认证相关
  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    java.util.Collections.emptyList[TransportClientBootstrap]
  }

  @volatile private var server: TransportServer = _

  private val stopped = new AtomicBoolean(false)

  /**
    * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
    * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
    */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
    * Remove the address's Outbox and stop it.
    */
  def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] = java.util.Collections.emptyList()

    server = transportContext.createServer(bindAddress, port, bootstraps)
    // netty server 启动后会注册 RpcEndpointVerifier ，用于远端验证是否可以连接
    log.trace("服务器启动后，注册RpcEndpointVerifier终端。。")
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // Message to a local RPC endpoint.
      try {
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: Exception => logDebug(e.getMessage)
      }
    } else {
      // Message to a remote RPC endpoint.
      postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
  }

  def createClient(address: String , port:Int, moduleName:String): TransportClient = {
    val inetAddress = InetAddress.getByName(address)
    val addresses = new InetSocketAddress(inetAddress,port)
    transportContext.createClient(addresses,moduleName)
  }

  def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case e: Exception => logDebug(s"Ignored failure: $e")
          case _ => logWarning(s"Ignored failure: $e")
        }
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        postToOutbox(message.receiver, rpcMessage)
        promise.future.failed.foreach {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      // 省略了取消的操作
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }


  /**
    * 从注册到dispatcher的列表中获取对应endpoint的引用，用于发送消息
    */
  override def endpointRef(endpoint: RpcEndpoint) = {
    dispatcher.getRpcEndpointRef(endpoint)
  }


  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    log.trace("初始化 master 的 endpointRef。。。。。。。。。。")
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    log.trace("初始化 RpcEndpointVerifierRef 发送验证消息：CheckExistence。。。。 ")
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new Exception(uri))
      }
    }(ThreadUtils.sameThread)
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    */
  override def stop(endpoint: RpcEndpointRef): Unit = {
    dispatcher.stop(endpoint)
  }

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  override def shutdown(): Unit = {
    cleanup()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }

    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
  }

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    */
  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  /**
    * Returns [[SerializationStream]] that forwards the serialized bytes to `out`.
    */
  def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  /**
    * Open a channel to download a file from the given URI. If the URIs returned by the
    * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
    * retrieve the files.
    *
    * @param uri URI with location of the file.
    */
  override def openChannel(uri: String): ReadableByteChannel = ???
}

object NettyRpcEnv extends Logging {
  /**
    * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
    * Use `currentEnv` to wrap the deserialization codes. E.g.,
    *
    * {{{
    *   NettyRpcEnv.currentEnv.withValue(this) {
    *     your deserialization codes
    *   }
    * }}}
    */
  val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
    * Similar to `currentEnv`, this variable references the client instance associated with an
    * RPC, in case it's needed to find out the remote address during deserialization.
    */
  val currentClient = new DynamicVariable[TransportClient](null)

}

class NettyRpcEnvFactory extends Logging {

  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
    new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        null, config.numUsableCores)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        // TODO
        log.trace("非clientMode , 启动netty server ....")
        nettyEnv.startServer(config.bindAddress, config.port)
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}


/**
  * The NettyRpcEnv version of RpcEndpointRef.
  *
  * This class behaves differently depending on where it's created. On the node that "owns" the
  * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
  *
  * On other machines that receive a serialized version of the reference, the behavior changes. The
  * instance will keep track of the TransportClient that sent the reference, so that messages
  * to the endpoint are sent over the client connection, instead of needing a new connection to
  * be opened.
  *
  * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
  * a client connection, since the process hosting the endpoint is not listening for incoming
  * connections. These refs should not be shared with 3rd parties, since they will not be able to
  * send messages to the endpoint.
  *
  * @param conf            Spark configuration.
  * @param endpointAddress The address where the endpoint is listening.
  * @param nettyEnv        The RpcEnv associated with this ref.
  */
class NettyRpcEndpointRef(
                           @transient private val conf: SparkConf,
                           private val endpointAddress: RpcEndpointAddress,
                           @transient @volatile private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(conf) {

  @transient
  @volatile var client: TransportClient = _

  override def address: RpcAddress =
    if (endpointAddress.rpcAddress != null) endpointAddress.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = endpointAddress.name

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode(): Int =
    if (endpointAddress == null) 0 else endpointAddress.hashCode()
}


/**
  * The message that is sent from the sender to the receiver.
  *
  * @param senderAddress the sender address. It's `null` if this message is from a client
  *                      `NettyRpcEnv`.
  * @param receiver      the receiver of this message.
  * @param content       the message content.
  */
class RequestMessage(
                      val senderAddress: RpcAddress,
                      val receiver: NettyRpcEndpointRef,
                      val content: Any) {

  /** Manually serialize [[RequestMessage]] to minimize the size. */
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new DataOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)
      val s = nettyEnv.serializeStream(out)
      try {
        s.writeObject(content)
      } finally {
        s.close()
      }
    } finally {
      out.close()
    }
    bos.toByteBuffer
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.host)
      out.writeInt(rpcAddress.port)
    }
  }

  override def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
}

object RequestMessage {

  private def readRpcAddress(in: DataInputStream): RpcAddress = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      RpcAddress(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)
    try {
      val senderAddress = readRpcAddress(in)
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
      val ref = new NettyRpcEndpointRef(nettyEnv.conf, endpointAddress, nettyEnv)
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref,
        // The remaining bytes in `bytes` are the message content.
        nettyEnv.deserialize(client, bytes))
    } finally {
      in.close()
    }
  }
}

/**
  * A response that indicates some failure happens in the receiver side.
  */
case class RpcFailure(e: Throwable)

/**
  * Dispatches incoming RPCs to registered endpoints.
  *
  * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
  * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
  * one that is not listening for incoming connections, but rather needs to be contacted via the
  * client socket).
  *
  * Events are sent on a per-connection basis, so if a client opens multiple connections to the
  * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
  * with different `RpcAddress` information).
  */
class NettyRpcHandler(
                       dispatcher: Dispatcher,
                       nettyEnv: NettyRpcEnv,
                       streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
                        client: TransportClient,
                        message: ByteBuffer,
                        callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
                        client: TransportClient,
                        message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }


  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      // If the remove RpcEnv listens to some address, we should also fire a
      // RemoteProcessConnectionError for the remote RpcEnv listening address
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null.
      // See java.net.Socket.getRemoteSocketAddress
      // Because we cannot get a RpcAddress, just log it
      logError("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      // If the remove RpcEnv listens to some address, we should also  fire a
      // RemoteProcessDisconnected for the remote RpcEnv listening address
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
      }
    } else {
      // If the channel is closed before connecting, its remoteAddress will be null. In this case,
      // we can ignore it since we don't fire "Associated".
      // See java.net.Socket.getRemoteSocketAddress
    }
  }
}