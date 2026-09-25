package com.github.h3yow.mqttsessiongatewayjava.gateway.session;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 会话表与按 clientId 分段的锁。
 *
 * <p>网关对同一个客户端的 connect/subscribe/publish-route/ack/disconnect
 * 复合操作在该客户端的分段锁内完成；不同客户端之间完全并行。
 */
public class SessionRegistry {

    private final ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    /**
     * 获取某客户端的分段锁（不存在则创建）。
     */
    public ReentrantLock lockFor(String clientId) {
        return locks.computeIfAbsent(clientId, k -> new ReentrantLock());
    }

    public ClientSession get(String clientId) {
        return sessions.get(clientId);
    }

    /**
     * 获取或创建会话。
     *
     * @param persistent       是否持久会话
     * @param pipelineCapacity 该客户端有界投递管道容量
     */
    public ClientSession getOrCreate(String clientId, boolean persistent, int pipelineCapacity) {
        return sessions.computeIfAbsent(clientId,
                k -> new ClientSession(clientId, persistent, pipelineCapacity));
    }

    /**
     * 彻底移除会话（cleanStart 接入旧持久会话时使用）。
     */
    public void remove(String clientId) {
        sessions.remove(clientId);
    }

    public Collection<ClientSession> all() {
        return sessions.values();
    }
}
