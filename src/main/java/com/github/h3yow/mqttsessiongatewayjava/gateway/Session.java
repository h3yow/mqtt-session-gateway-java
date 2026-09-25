package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 客户端会话：连接状态、订阅集合、离线积压与待确认消息。
 * 仅由 Gateway 在持锁状态下访问，自身不做并发控制。
 */
final class Session {

    private final String clientId;
    private final boolean persistent;
    private boolean online;

    /** 按过滤器去重的订阅集合（重复订阅同一过滤器视为更新 QoS）。 */
    private final Map<String, Subscription> subscriptions = new LinkedHashMap<>();

    /** 离线积压条目：消息 + 投递时应采用的 QoS。 */
    record BacklogEntry(Message message, QoS qos) {
    }

    /** 离线积压：尚未投递的消息。 */
    private final List<BacklogEntry> backlog = new ArrayList<>();

    /** 已投递待确认（至少一次）：messageId -> Message，重连后需重投。 */
    private final Map<Long, Message> pendingAck = new LinkedHashMap<>();

    Session(String clientId, boolean persistent) {
        this.clientId = clientId;
        this.persistent = persistent;
    }

    String clientId() {
        return clientId;
    }

    boolean isPersistent() {
        return persistent;
    }

    boolean isOnline() {
        return online;
    }

    void markOnline() {
        this.online = true;
    }

    void markOffline() {
        this.online = false;
    }

    void addSubscription(Subscription sub) {
        subscriptions.put(sub.filter().raw(), sub);
    }

    List<Subscription> subscriptions() {
        return List.copyOf(subscriptions.values());
    }

    /** 离线时入积压队列。 */
    void enqueueBacklog(Message message, QoS qos) {
        backlog.add(new BacklogEntry(message, qos));
    }

    /** 在线时登记为待确认（至少一次语义）。 */
    void addPendingAck(Message message) {
        pendingAck.put(message.id(), message);
    }

    /** 重连上线：取走全部离线积压。 */
    List<BacklogEntry> drainBacklog() {
        List<BacklogEntry> out = new ArrayList<>(backlog);
        backlog.clear();
        return out;
    }

    /** 重连上线：收集需重投的未确认消息（仍保留在待确认集合中）。 */
    List<Message> pendingForRedelivery() {
        return List.copyOf(pendingAck.values());
    }

    int backlogSize() {
        return backlog.size();
    }

    int pendingAckCount() {
        return pendingAck.size();
    }

    void removePending(long messageId) {
        pendingAck.remove(messageId);
    }

    boolean hasPending(long messageId) {
        return pendingAck.containsKey(messageId);
    }
}
