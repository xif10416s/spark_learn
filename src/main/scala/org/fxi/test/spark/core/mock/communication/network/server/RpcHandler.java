package org.fxi.test.spark.core.mock.communication.network.server;


import org.apache.spark.network.client.StreamCallbackWithID;

import org.fxi.test.spark.core.mock.communication.network.client.RpcResponseCallback;
import org.fxi.test.spark.core.mock.communication.network.client.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 *  处理来自sendRPC的消息
 */
public abstract class RpcHandler {
    private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();
    private static class OneWayRpcCallback implements RpcResponseCallback {

        private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);


        public void onSuccess(ByteBuffer response) {
            logger.warn("Response provided for one-way RPC.");
        }


        public void onFailure(Throwable e) {
            logger.error("Error response provided for one-way RPC.", e);
        }
    }

    /**
     * 接收Rpc消息，任何异常都会被当作标准RPC错误返回给client
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *               of this RPC. This will always be the exact same object for a particular channel.
     * @param message The serialized bytes of the RPC.
     * @param callback Callback which should be invoked exactly once upon success or failure of the
     *                 RPC.
     */
    public abstract void receive(
            TransportClient client,
            ByteBuffer message,
            RpcResponseCallback callback);


    public void receive(TransportClient client, ByteBuffer message) {
        receive(client, message, ONE_WAY_CALLBACK);
    }

    /**
     * Invoked when the channel associated with the given client is active.
     */
    public void channelActive(TransportClient client) { }

    /**
     * Invoked when the channel associated with the given client is inactive.
     * No further requests will come from this client.
     */
    public void channelInactive(TransportClient client) { }

    public void exceptionCaught(Throwable cause, TransportClient client) { }

    /**
     * TODO
     * @param client
     * @param messageHeader
     * @param callback
     * @return
     */
    public StreamCallbackWithID receiveStream(
            TransportClient client,
            ByteBuffer messageHeader,
            org.apache.spark.network.client.RpcResponseCallback callback) {
        throw new UnsupportedOperationException();
    }
}
