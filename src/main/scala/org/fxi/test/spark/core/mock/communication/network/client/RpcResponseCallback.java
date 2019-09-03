package org.fxi.test.spark.core.mock.communication.network.client;

import java.nio.ByteBuffer;

/**
 * rpc 回调函数，onSuccess 和 onFailure 会被调用其中一个
 */
public interface RpcResponseCallback {
    /**
     * Successful serialized result from server.
     *
     * rpc成功回调函数
     */
    void onSuccess(ByteBuffer response);

    /** rpc异常回调函数 */
    void onFailure(Throwable e);
}
