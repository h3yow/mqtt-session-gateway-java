package com.github.h3yow.mqttsessiongatewayjava.gateway.storage;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;

import java.util.List;
import java.util.Map;

/**
 * 关闭持久化时使用的空实现：所有状态仅存内存，重启即失。
 */
public final class NoopDurableStore implements DurableStore {

    @Override
    public void recordSession(String clientId) {
    }

    @Override
    public void recordSubscription(String clientId, String filter, Qos qos) {
    }

    @Override
    public void recordRemoveSubscription(String clientId, String filter) {
    }

    @Override
    public void recordRemoveSession(String clientId) {
    }

    @Override
    public void recordQueued(String clientId, long packetId, String topic, Qos qos, byte[] payload) {
    }

    @Override
    public void recordAcked(String clientId, long packetId) {
    }

    @Override
    public Snapshot loadSnapshot() {
        return new Snapshot(List.of(), Map.of(), Map.of(), Map.of());
    }

    @Override
    public void close() {
    }
}
