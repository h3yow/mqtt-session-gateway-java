package com.github.h3yow.mqttsessiongatewayjava.gateway.persist;

import java.util.List;

/**
 * 网关状态存储：重放追加日志与全量快照压缩。
 * 所有调用均由 Gateway 持锁发起，实现自身无需考虑并发。
 */
public interface GatewayStore extends AutoCloseable {

    /**
     * 启动时重放：优先重放快照，再重放其后的追加变更，返回合并后的当前状态。
     */
    StoredState.Snapshot replay();

    /** 原子追加一批变更（同一操作内的多条状态变更必须一起生效）。 */
    void appendBatch(List<StoredState.Change> changes);

    /** 用全量快照压缩存储（重放后调用，防止日志无限增长）。 */
    void compact(StoredState.Snapshot snapshot);

    @Override
    void close();
}
