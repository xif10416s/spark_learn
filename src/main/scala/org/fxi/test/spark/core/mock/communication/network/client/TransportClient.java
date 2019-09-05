package org.fxi.test.spark.core.mock.communication.network.client;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.spark.network.buffer.NioManagedBuffer;


import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RpcRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

public class TransportClient implements Closeable {
    private final Channel channel;
    private final TransportResponseHandler handler;

    public TransportClient(Channel channel, TransportResponseHandler handler) {
        this.channel = Preconditions.checkNotNull(channel);
        this.handler = Preconditions.checkNotNull(handler);
    }

    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    public void close() throws IOException {
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    public Channel getChannel() {
        return channel;
    }

    public void removeRpcRequest(long requestId) {
        handler.removeRpcRequest(requestId);
    }

    /**
     * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
     * with the server's response or upon any failure.
     *
     * @param message The message to send.
     * @param callback Callback to handle the RPC's reply.
     * @return The RPC's id.
     */
    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        logger.trace(TransportClient.class + " Sending RPC to {}", getRemoteAddress(channel));
        long requestId = requestId();
        handler.addRpcRequest(requestId, callback);


        channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message))).addListener(new ChannelListener(requestId));

        return requestId;
    }

    public SocketAddress getSocketAddress() {
        return channel.remoteAddress();
    }

    private static long requestId() {
        return Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }

    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }

    /**
     * 监听channel writeAndFlush成功事件
     */
    class ChannelListener implements GenericFutureListener<Future<? super Void>> {
        final long startTime;
        final Object requestId;

        ChannelListener(Object requestId) {
            this.startTime = System.currentTimeMillis();
            this.requestId = requestId;
        }
        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
            if (future.isSuccess()) {
                long timeTaken = System.currentTimeMillis() - startTime;
                logger.info("TransportClient.sendRpc cost time : " + timeTaken);
            }
        }
    }
}

