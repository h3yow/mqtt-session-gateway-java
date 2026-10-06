package com.github.h3yow.mqttsessiongatewayjava.gateway.persist;

import java.util.List;
import java.util.Map;

/**
 * 持久化快照/重放所用的纯数据结构（JSON 可直接序列化）。
 * 仅描述持久会话：非持久会话不落盘。
 */
public final class StoredState {

    private StoredState() {
    }

    /** 已持久化的消息（离线积压或待确认）。 */
    public record StoredMessage(long id, String topic, String payload) {
    }

    /** 一条订阅：过滤器原文 + 订阅 QoS 名称。 */
    public record StoredSubscription(String filter, String qos) {
    }

    /** 一条离线积压条目：消息 + 协商后的投递 QoS 名称。 */
    public record StoredBacklogEntry(StoredMessage message, String qos) {
    }

    /** 一个持久会话的完整状态（恢复后一律为离线）。 */
    public record StoredSession(
            String clientId,
            List<StoredSubscription> subscriptions,
            List<StoredBacklogEntry> backlog,
            List<StoredMessage> pendingAck,
            List<Long> ackedIds) {
    }

    /** 全量快照：会话表 + 已使用的最大消息 id。 */
    public record Snapshot(Map<String, StoredSession> sessions, long maxMessageId) {
    }

    /** 一次原子状态变更（追加日志中的一行）。具体字段按 {@link #type} 解释。 */
    public record Change(
            String type,
            String clientId,
            StoredSubscription subscription,
            StoredBacklogEntry backlogEntry,
            StoredMessage pendingMessage,
            StoredMessage backlogMessage,
            Long ackedId,
            Long messageId) {

        public static Change persistSession(String clientId) {
            return new Change("PERSIST_SESSION", clientId, null, null, null, null, null, null);
        }

        public static Change addSubscription(String clientId, StoredSubscription subscription) {
            return new Change("ADD_SUBSCRIPTION", clientId, subscription, null, null, null, null, null);
        }

        public static Change enqueueBacklog(String clientId, StoredBacklogEntry entry) {
            return new Change("ADD_BACKLOG", clientId, null, entry, null, null, null, null);
        }

        public static Change addPending(String clientId, StoredMessage message) {
            return new Change("ADD_PENDING", clientId, null, null, message, null, null, null);
        }

        public static Change drainBacklog(String clientId, StoredMessage message) {
            return new Change("REMOVE_BACKLOG", clientId, null, null, null, message, null, null);
        }

        public static Change removePending(String clientId, long messageId) {
            return new Change("REMOVE_PENDING", clientId, null, null, null, null, null, messageId);
        }

        public static Change addAckedId(String clientId, long messageId) {
            return new Change("ADD_ACKED_ID", clientId, null, null, null, null, messageId, null);
        }
    }
}
