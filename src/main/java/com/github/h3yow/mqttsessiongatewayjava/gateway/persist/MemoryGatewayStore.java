package com.github.h3yow.mqttsessiongatewayjava.gateway.persist;

import java.util.List;
import java.util.Map;

/**
 * 纯内存存储（dataDir 为空时使用）：不持久化任何内容，进程重启后状态丢失。
 */
public class MemoryGatewayStore implements GatewayStore {

    @Override
    public StoredState.Snapshot replay() {
        return new StoredState.Snapshot(Map.of(), 0L);
    }

    @Override
    public void appendBatch(List<StoredState.Change> changes) {
        // 内存模式：丢弃所有变更
    }

    @Override
    public void compact(StoredState.Snapshot snapshot) {
        // 内存模式无需压缩
    }

    @Override
    public void close() {
        // 无资源需释放
    }
}
