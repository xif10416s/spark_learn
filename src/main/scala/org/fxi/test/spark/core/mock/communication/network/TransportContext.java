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

package org.fxi.test.spark.core.mock.communication.network;

import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;


import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;

import org.apache.spark.network.util.NettyUtils;

import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

import org.fxi.test.spark.core.mock.communication.network.client.TransportClient;
import org.fxi.test.spark.core.mock.communication.network.client.TransportResponseHandler;
import org.fxi.test.spark.core.mock.communication.network.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 *
 */
public class TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(org.apache.spark.network.TransportContext.class);

  private final TransportConf conf;
  private final RpcHandler rpcHandler;
  private final boolean closeIdleConnections;

  private final TransportContext instance ;
  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;
    this.instance = this;
  }


  /** Create a server which will attempt to bind to a specific port. */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port. */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, new ArrayList<TransportServerBootstrap>());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
//        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, 1L);
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      3000, closeIdleConnections);
//    return null;
  }

  public TransportConf getConf() { return conf; }

  /**
   *  省略了工厂类，以及连接池等优化处理，直接创建新的客户端
   *  实际上是通过clientFactory工厂类，做一些连接池管理和优化缓存处理
   * */
  public TransportClient createClient(InetSocketAddress address , String clientName)
          throws IOException, InterruptedException {
    logger.debug("Creating new connection to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    EventLoopGroup workerGroup = new NioEventLoopGroup(3, new DefaultThreadFactory(clientName+ "-client", true));
    bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            // Disable Nagle's Algorithm since we don't want packets to wait
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true);


    // 省略了一些 netty 通讯的配置

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = instance.initializePipeline(ch,rpcHandler);
        System.out.println("========================"+clientHandler);
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.await(3000)) {
      throw new IOException(
              String.format("Connecting to %s timed out (%s ms)", address, 3000));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    // clientBootstraps 配置 省略
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
            address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }
}
