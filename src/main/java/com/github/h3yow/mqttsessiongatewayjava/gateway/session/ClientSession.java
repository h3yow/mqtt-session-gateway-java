package com.github.h3yow.mqttsessiongatewayjava.gateway.session;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.BoundedDeliveryQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个客户端的会话状态。
 *
 * <p>会话复合状态（online/subscriptions/inflight/pipeline）的控制面变更由
 * {@link SessionRegistry} 中以 clientId 分段的锁保护；worker 在“投递成功→
 * 登记 inflight”这一步与断连流程通过 {@link #completionLock()} 互斥，
 * 避免消息在断连瞬间漏归并。
 */
public class ClientSession {

    private final String clientId;
    private final boolean persistent;

    private volatile boolean online;
    private final Map<String, Qos> subscriptions = new LinkedHashMap<>();
    private final Map<Long, QueuedMessage> inflight = new LinkedHashMap<>();
    /**
     * 有界投递管道：容量为配置上限。发布线程以 {@code offer(timeout)} 入队，
     * 慢客户端消费不动时入队会超时并得到明确的背压拒绝；worker 失败重试走队首。
     */
    private final BoundedDeliveryQueue<QueuedMessage> pipeline;
    private final AtomicLong packetIdSequence = new AtomicLong(0);
    private final ReentrantLock completionLock = new ReentrantLock();
    /** 已在 WAL 中存在 QUEUE 记录的 packetId，避免恢复后重复写盘。 */
    private final Set<Long> durablePendingPacketIds = ConcurrentHashMap.newKeySet();
    /** 已成功投递的 QoS0 packetId（仅用于区分错误确认原因，不参与重传）。 */
    private final Set<Long> deliveredQos0PacketIds = ConcurrentHashMap.newKeySet();

    public ClientSession(String clientId, boolean persistent, int pipelineCapacity) {
        this.clientId = clientId;
        this.persistent = persistent;
        this.pipeline = new BoundedDeliveryQueue<>(pipelineCapacity);
    }

    public String clientId() {
        return clientId;
    }

    public boolean persistent() {
        return persistent;
    }

    public boolean online() {
        return online;
    }

    public void online(boolean value) {
        this.online = value;
    }

    public Map<String, Qos> subscriptions() {
        return subscriptions;
    }

    public Map<Long, QueuedMessage> inflight() {
        return inflight;
    }

    public BoundedDeliveryQueue<QueuedMessage> pipeline() {
        return pipeline;
    }

    public AtomicLong packetIdSequence() {
        return packetIdSequence;
    }

    /** worker 登记投递结果 与 断连回灌 inflight 之间的互斥锁。 */
    public ReentrantLock completionLock() {
        return completionLock;
    }

    /** 该消息是否已在持久层记录（未 ACK）：首次标记返回 true。 */
    public boolean markDurableIfAbsent(long packetId) {
        return durablePendingPacketIds.add(packetId);
    }

    /** 确认后从持久待决集合移除。 */
    public void clearDurableMark(long packetId) {
        durablePendingPacketIds.remove(packetId);
    }

    /** 记录已成功投递的 QoS0 packetId。 */
    public void addDeliveredQos0(long packetId) {
        deliveredQos0PacketIds.add(packetId);
    }

    /** 判断该 packetId 是否为已投递的 QoS0 消息。 */
    public boolean isDeliveredQos0(long packetId) {
        return deliveredQos0PacketIds.contains(packetId);
    }
}
