package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

/**
 * 网关持久状态快照：用于重启恢复（仅包含持久会话）。
 *
 * @param lastMessageId 已分配的最大消息 id（恢复后继续递增，不复用已确认消息的 id）
 * @param sessions      全部持久会话的状态
 */
public record GatewaySnapshot(long lastMessageId, List<SessionSnapshot> sessions) {

    /**
     * 单个持久会话的落盘状态。恢复后会话一律为离线状态，等待客户端重连。
     */
    public record SessionSnapshot(
            String clientId,
            List<SubscriptionSnapshot> subscriptions,
            List<BacklogEntrySnapshot> backlog,
            List<Message> pendingAck,
            List<Long> ackedIds) {
    }

    /** 订阅快照：过滤器原文 + 订阅 QoS。 */
    public record SubscriptionSnapshot(String filter, QoS qos) {
    }

    /** 离线积压条目快照：消息 + 协商后的投递 QoS。 */
    public record BacklogEntrySnapshot(Message message, QoS qos) {
    }
}
