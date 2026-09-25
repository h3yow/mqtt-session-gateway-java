package com.github.h3yow.mqttsessiongatewayjava.gateway.storage;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;

import java.util.List;
import java.util.Map;

/**
 * 持久化存储：基于追加 WAL + 快照，保证网关重启后
 * 持久会话、订阅与未确认消息可恢复。
 */
public interface DurableStore {

    void recordSession(String clientId);

    void recordSubscription(String clientId, String filter, Qos qos);

    void recordRemoveSubscription(String clientId, String filter);

    void recordRemoveSession(String clientId);

    void recordQueued(String clientId, long packetId, String topic, Qos qos, byte[] payload);

    void recordAcked(String clientId, long packetId);

    /**
     * 读取全部持久状态，用于启动恢复。
     */
    Snapshot loadSnapshot();

    void close();

    /**
     * 启动恢复视图。
     */
    record Snapshot(
            List<String> sessions,
            Map<String, List<SubEntry>> subscriptions,
            Map<String, List<MsgEntry>> queued,
            Map<String, Long> nextPacketIds) {
    }

    record SubEntry(String filter, Qos qos) {
    }

    record MsgEntry(long packetId, String topic, Qos qos, byte[] payload) {
    }
}

