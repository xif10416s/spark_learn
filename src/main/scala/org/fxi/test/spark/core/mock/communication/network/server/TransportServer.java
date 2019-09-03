package org.fxi.test.spark.core.mock.communication.network.server;

import com.codahale.metrics.MetricSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.SystemUtils;

import org.apache.spark.network.util.JavaUtils;
import org.fxi.test.spark.core.mock.communication.network.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TransportServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

    private final RpcHandler appRpcHandler;

    private final List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;

    private int port = -1;

    private String moduleName ;

    private int numThreads = 2;

    private TransportContext context;

    private ChannelFuture channelFuture;

    public TransportServer(
            TransportContext context,
            String hostToBind,
            int portToBind,
            RpcHandler appRpcHandler,
            List<TransportServerBootstrap> bootstraps) {
        this.context = context;
        this.moduleName = context.getConf().getModuleName();
        this.appRpcHandler = appRpcHandler;
        this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

        boolean shouldClose = true;
        try {
            init(hostToBind, portToBind);
            shouldClose = false;
        } finally {
            if (shouldClose) {
                JavaUtils.closeQuietly(this);
            }
        }
    }

    public int getPort() {
        if (port == -1) {
            throw new IllegalStateException("Server not initialized");
        }
        return port;
    }

    /**
     * netty server 初始化
     * @param hostToBind
     * @param portToBind
     */
    private void init(String hostToBind, int portToBind) {

        EventLoopGroup bossGroup = new NioEventLoopGroup(numThreads, new DefaultThreadFactory(moduleName+ "-server", true));
        EventLoopGroup workerGroup = bossGroup;

        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS);



        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                RpcHandler rpcHandler = appRpcHandler;
                for (TransportServerBootstrap bootstrap : bootstraps) {
                    rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
                }
                context.initializePipeline(ch, rpcHandler);
            }
        });

        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
        ChannelFuture channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        logger.debug("Shuffle server started on port: {}", port);
        channelFuture.channel().closeFuture().syncUninterruptibly();
    }



    public void close() {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }
}
