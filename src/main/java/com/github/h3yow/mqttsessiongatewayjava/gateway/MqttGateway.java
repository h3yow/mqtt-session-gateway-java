package com.github.h3yow.mqttsessiongatewayjava.gateway;

import com.github.h3yow.mqttsessiongatewayjava.gateway.config.GatewayProperties;
import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.DeliveryDispatcher;
import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.DeviceDeliveryHandler;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.DeliveryDecision;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.OperationResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishRequest;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.SubscriberOutcome;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Subscription;
import com.github.h3yow.mqttsessiongatewayjava.gateway.session.ClientSession;
import com.github.h3yow.mqttsessiongatewayjava.gateway.session.SessionRegistry;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.DurableStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.topic.InvalidTopicException;
import com.github.h3yow.mqttsessiongatewayjava.gateway.topic.TopicMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 进程内 MQTT 风格设备消息网关门面。
 *
 * <h3>线程模型</h3>
 * <ul>
 *   <li>控制面（connect/subscribe/unsubscribe/ack/disconnect）在调用方线程执行，
 *       同一 clientId 的复合状态变更由分段锁串行化，不同客户端完全并行。</li>
 *   <li>publish 在调用方线程做只读路由快照收集，再逐订阅者在各自分段锁内入队，
 *       绝不在发布线程上执行设备 IO。</li>
 *   <li>每个在线客户端独占一个 worker 线程执行设备投递；慢/故障客户端只影响自己。</li>
 * </ul>
 *
 * <h3>语义</h3>
 * <ul>
 *   <li>QoS0：至多一次。投递即忘，不进 inflight、不做确认、失败即放弃。</li>
 *   <li>QoS1：至少一次。成功后进 inflight，等待 PUBACK；断连未确认的消息
 *       在重连/重启后以 redelivery 标志重投，由去重（packetId）保证不重复确认。</li>
 *   <li>有效投递 QoS = min(发布 QoS, 订阅 QoS)；同一客户端多个重叠过滤器只投递一次，
 *       取匹配订阅中的最高 QoS。</li>
 * </ul>
 */
public class MqttGateway {

    private static final Logger log = LoggerFactory.getLogger(MqttGateway.class);

    private final GatewayProperties properties;
    private final DurableStore store;
    private final SessionRegistry registry;
    private final DeliveryDispatcher dispatcher;

    public MqttGateway(GatewayProperties properties,
                       DurableStore store,
                       DeviceDeliveryHandler handler) {
        this.properties = properties;
        this.store = store;
        this.registry = new SessionRegistry();
        this.dispatcher = new DeliveryDispatcher(properties, handler);
        recover();
    }

    // ------------------------------------------------------------------
    // 恢复
    // ------------------------------------------------------------------

    private void recover() {
        DurableStore.Snapshot snapshot = store.loadSnapshot();
        for (String clientId : snapshot.sessions()) {
            List<DurableStore.SubEntry> subs =
                    snapshot.subscriptions().getOrDefault(clientId, List.of());
            List<DurableStore.MsgEntry> msgs =
                    snapshot.queued().getOrDefault(clientId, List.of());
            // 已持久化的积压不得因容量配置调小而丢失：恢复容量取配置与历史积压的较大值
            int capacity = Math.max(properties.getMaxBacklogPerSession(), msgs.size() + 1);
            ClientSession session = registry.getOrCreate(clientId, true, capacity);
            for (DurableStore.SubEntry s : subs) {
                session.subscriptions().put(s.filter(), s.qos());
            }
            Long next = snapshot.nextPacketIds().get(clientId);
            if (next != null) {
                session.packetIdSequence().set(next);
            }
            // 未确认积压（含 inflight 与未投出）：以 redelivery 形式回到管道
            for (DurableStore.MsgEntry m : msgs) {
                QueuedMessage qm = new QueuedMessage(m.packetId(), m.topic(),
                        m.payload(), m.qos(), true, System.currentTimeMillis());
                session.pipeline().add(qm);
                session.markDurableIfAbsent(m.packetId());
            }
            log.info("恢复持久会话: client={} 订阅={} 条 积压={} 条 nextPacketId={}",
                    clientId, subs.size(), msgs.size(),
                    session.packetIdSequence().get());
        }
    }

    // ------------------------------------------------------------------
    // 连接生命周期
    // ------------------------------------------------------------------

    public OperationResult connect(String clientId, boolean cleanStart) {
        if (clientId == null || clientId.isEmpty()) {
            return reject(RejectReason.UNKNOWN_CLIENT, "clientId 不能为空");
        }
        var lock = registry.lockFor(clientId);
        lock.lock();
        try {
            ClientSession existing = registry.get(clientId);
            if (existing != null && existing.online()) {
                // MQTT 风格：同 clientId 再次连接视为顶替旧连接
                doOffline(existing, false);
            }
            if (cleanStart) {
                if (existing != null) {
                    // 丢弃该 clientId 的全部历史状态
                    dispatcher.stop(clientId);
                    registry.remove(clientId);
                    if (existing.persistent()) {
                        store.recordRemoveSession(clientId);
                    }
                }
                ClientSession fresh = registry.getOrCreate(clientId, false, properties.getMaxBacklogPerSession());
                fresh.online(true);
                dispatcher.start(fresh);
                log.info("客户端接入(cleanStart): client={}", clientId);
                return OperationResults.ok("cleanStart 会话已建立");
            }

            // 持久会话：不存在则新建；存在则恢复在线
            ClientSession session = registry.getOrCreate(clientId, true, properties.getMaxBacklogPerSession());
            if (existing == null) {
                store.recordSession(clientId);
            }
            // 先置在线状态再启动 worker：worker 投递成功登记 inflight 时复查 online 必为 true
            session.online(true);
            int backlog = session.pipeline().size();
            dispatcher.start(session);
            log.info("客户端接入(持久会话): client={} 待投积压={} 订阅={}",
                    clientId, backlog, session.subscriptions().size());
            return OperationResults.ok(
                    "持久会话已恢复，待投积压 " + backlog + " 条");
        } finally {
            lock.unlock();
        }
    }

    public OperationResult disconnect(String clientId) {
        var lock = registry.lockFor(clientId);
        lock.lock();
        try {
            ClientSession session = requireOnline(clientId);
            if (session == null) {
                return reject(RejectReason.UNKNOWN_CLIENT,
                        "客户端不存在或不在线: " + clientId);
            }
            doOffline(session, !session.persistent());
            return OperationResults.ok(
                    session.persistent() ? "已断连，持久会话保留" : "已断连");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将在线会话置为离线状态。
     *
     * @param drop true 表示彻底销毁（非持久会话）；false 表示保留持久状态
     */
    private void doOffline(ClientSession session, boolean drop) {
        String clientId = session.clientId();
        dispatcher.stop(clientId);
        session.completionLock().lock();
        try {
            session.online(false);
            // 1) inflight（设备已收但未 PUBACK）→ 回队首，重连时优先重投（redelivery）
            for (QueuedMessage m : new ArrayList<>(session.inflight().values())) {
                try {
                    session.pipeline().putFront(toRedelivery(m));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("断连回灌被中断: " + clientId, e);
                }
            }
            session.inflight().clear();
            // 2) 管道中尚未投出的 QoS1 消息保持原位（pipeline 不销毁）
            if (drop) {
                registry.remove(clientId);
                log.info("非持久会话已销毁: client={}", clientId);
            } else {
                log.info("持久会话离线保留: client={} 积压={} 条",
                        clientId, session.pipeline().size());
            }
        } finally {
            session.completionLock().unlock();
        }
    }

    // ------------------------------------------------------------------
    // 订阅
    // ------------------------------------------------------------------

    public OperationResult subscribe(String clientId, String filter, Qos qos) {
        if (qos == null) {
            return reject(RejectReason.ILLEGAL_LEVEL_CHARACTER, "qos 不能为空");
        }
        String normalized;
        try {
            normalized = TopicMatcher.validateFilter(filter);
        } catch (InvalidTopicException e) {
            log.info("订阅被拒绝: client={} filter={} reason={}",
                    clientId, filter, e.reason());
            return reject(e.reason(), e.getMessage());
        }
        var lock = registry.lockFor(clientId);
        lock.lock();
        try {
            ClientSession session = requireOnline(clientId);
            if (session == null) {
                return reject(RejectReason.UNKNOWN_CLIENT,
                        "客户端不存在或不在线: " + clientId);
            }
            session.subscriptions().put(normalized, qos);
            if (session.persistent()) {
                store.recordSubscription(clientId, normalized, qos);
            }
            log.info("订阅生效: client={} filter={} qos={}", clientId, normalized, qos);
            return OperationResults.ok("订阅成功");
        } finally {
            lock.unlock();
        }
    }

    public OperationResult unsubscribe(String clientId, String filter) {
        String normalized;
        try {
            normalized = TopicMatcher.validateFilter(filter);
        } catch (InvalidTopicException e) {
            return reject(e.reason(), e.getMessage());
        }
        var lock = registry.lockFor(clientId);
        lock.lock();
        try {
            ClientSession session = requireOnline(clientId);
            if (session == null) {
                return reject(RejectReason.UNKNOWN_CLIENT,
                        "客户端不存在或不在线: " + clientId);
            }
            if (!session.subscriptions().containsKey(normalized)) {
                return reject(RejectReason.NOT_SUBSCRIBED,
                        "客户端未订阅该过滤器: " + normalized);
            }
            session.subscriptions().remove(normalized);
            if (session.persistent()) {
                store.recordRemoveSubscription(clientId, normalized);
            }
            log.info("取消订阅: client={} filter={}", clientId, normalized);
            return OperationResults.ok("取消订阅成功");
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 发布路由
    // ------------------------------------------------------------------

    public PublishResult publish(PublishRequest request) {
        String topic;
        try {
            topic = TopicMatcher.validateTopic(request.topic());
        } catch (InvalidTopicException e) {
            throw e;
        }

        // 第一阶段：在各分段锁内收集匹配订阅快照（不做入队，持锁时间极短）
        List<RouteTarget> targets = new ArrayList<>();
        for (ClientSession session : registry.all()) {
            var lock = registry.lockFor(session.clientId());
            lock.lock();
            try {
                Qos best = null;
                for (Map.Entry<String, Qos> e : session.subscriptions().entrySet()) {
                    if (TopicMatcher.matches(e.getKey(), topic)) {
                        if (best == null || e.getValue().ordinal() > best.ordinal()) {
                            best = e.getValue();
                        }
                    }
                }
                if (best != null) {
                    targets.add(new RouteTarget(session.clientId(), best));
                }
            } finally {
                lock.unlock();
            }
        }

        // 第二阶段：逐订阅者判定与入队
        List<SubscriberOutcome> outcomes = new ArrayList<>(targets.size());
        for (RouteTarget target : targets) {
            Qos effective = minQos(request.qos(), target.subscribedQos());
            outcomes.add(routeOne(target.clientId(), topic, request, effective));
        }
        log.info("路由完成: topic={} publishQos={} 命中订阅者={}",
                topic, request.qos(), targets.size());
        return new PublishResult(topic, request.qos(), List.copyOf(outcomes));
    }

    private SubscriberOutcome routeOne(String clientId, String topic,
                                       PublishRequest request, Qos effective) {
        var lock = registry.lockFor(clientId);
        lock.lock();
        try {
            ClientSession session = registry.get(clientId);
            if (session == null) {
                return outcome(clientId, DeliveryDecision.SKIPPED, null, null,
                        "会话已消失");
            }
            // 容量上限：未投出管道 + inflight 视为该会话占用的积压
            int occupied = session.pipeline().size() + session.inflight().size();
            if (occupied >= properties.getMaxBacklogPerSession()) {
                log.warn("积压超过上限，拒绝入队: client={} occupied={} cap={}",
                        clientId, occupied, properties.getMaxBacklogPerSession());
                return outcome(clientId, DeliveryDecision.REJECTED, null,
                        RejectReason.BACKLOG_FULL,
                        "积压 " + occupied + " 已达上限 "
                                + properties.getMaxBacklogPerSession());
            }

            boolean offline = !session.online();
            if (offline && !session.persistent()) {
                // 非持久会话离线：不投递、不占序号，明确告知，绝不静默丢弃
                return outcome(clientId, DeliveryDecision.SKIPPED, null, null,
                        "非持久会话离线，不保留消息");
            }
            if (offline && effective == Qos.AT_MOST_ONCE) {
                // QoS0 至多一次：离线不积压、不占序号，明确给出判定（非静默丢弃）
                return outcome(clientId, DeliveryDecision.SKIPPED, null, null,
                        "持久会话离线且消息为 QoS0，至多一次语义不积压");
            }

            // 只有真正进入投递/积压的消息才分配 per-client packetId
            long packetId = nextPacketId(session);
            // 会话离线期间入队的 QoS1 消息，将来投出时必然是一次（重）投递
            QueuedMessage msg = new QueuedMessage(packetId, topic, request.payload(),
                    effective, offline, System.currentTimeMillis());

            if (offline) {
                // 离线持久会话 + QoS1：入积压并落盘
                if (!offerWithBackpressure(session, msg)) {
                    return outcome(clientId, DeliveryDecision.REJECTED, packetId,
                            RejectReason.BACKPRESSURE_TIMEOUT,
                            "背压：等待管道空位超时");
                }
                if (session.markDurableIfAbsent(packetId)) {
                    store.recordQueued(clientId, packetId, topic, effective,
                            request.payload());
                }
                log.info("离线积压: client={} packetId={} topic={} 积压={}",
                        clientId, packetId, topic, session.pipeline().size());
                return outcome(clientId, DeliveryDecision.STORED_OFFLINE, packetId,
                        null, "客户端离线，已进入持久积压");
            }

            // 在线：入 pipeline 交给独占 worker；带超时的背压
            if (!offerWithBackpressure(session, msg)) {
                return outcome(clientId, DeliveryDecision.REJECTED, packetId,
                        RejectReason.BACKPRESSURE_TIMEOUT,
                        "背压：慢客户端管道满，等待超时");
            }
            if (session.persistent() && effective == Qos.AT_LEAST_ONCE
                    && session.markDurableIfAbsent(packetId)) {
                store.recordQueued(clientId, packetId, topic, effective,
                        request.payload());
            }
            return outcome(clientId, DeliveryDecision.DELIVERED, packetId, null,
                    "已投递至客户端管道，有效QoS=" + effective);
        } finally {
            lock.unlock();
        }
    }

    private boolean offerWithBackpressure(ClientSession session, QueuedMessage msg) {
        try {
            boolean ok = session.pipeline().offer(
                    msg, properties.getEnqueueTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return ok;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ------------------------------------------------------------------
    // 确认
    // ------------------------------------------------------------------

    public OperationResult acknowledge(String clientId, long packetId) {
        var lock = registry.lockFor(clientId);
        lock.lock();
        try {
            ClientSession session = registry.get(clientId);
            if (session == null) {
                return reject(RejectReason.UNKNOWN_CLIENT,
                        "客户端不存在: " + clientId);
            }
            QueuedMessage inflightMsg = session.inflight().get(packetId);
            if (inflightMsg != null) {
                if (inflightMsg.qos() == Qos.AT_MOST_ONCE) {
                    return reject(RejectReason.QOS0_DOES_NOT_ACK,
                            "QoS0 消息不存在确认语义: packetId=" + packetId);
                }
                session.inflight().remove(packetId);
                if (session.persistent()) {
                    store.recordAcked(clientId, packetId);
                    session.clearDurableMark(packetId);
                }
                log.info("确认成功: client={} packetId={} redelivery={}",
                        clientId, packetId, inflightMsg.redelivery());
                return OperationResults.ok("PUBACK 已接受");
            }
            // 不在 inflight：区分“QoS0 无确认语义”“重复确认”“未投递给本会话”
            if (session.isDeliveredQos0(packetId)) {
                return reject(RejectReason.QOS0_DOES_NOT_ACK,
                        "QoS0 消息不存在确认语义: packetId=" + packetId);
            }
            if (packetId < 0 || packetId >= session.packetIdSequence().get()) {
                return reject(RejectReason.PACKET_NOT_IN_FLIGHT,
                        "该 packetId 未投递给本会话: " + packetId
                                + "（下一个packetId=" + session.packetIdSequence().get() + ")");
            }
            return reject(RejectReason.DUPLICATE_ACK,
                    "该 packetId 已被确认过: " + packetId);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 关闭
    // ------------------------------------------------------------------

    public void shutdown() {
        dispatcher.shutdown();
        store.close();
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    private ClientSession requireOnline(String clientId) {
        ClientSession session = registry.get(clientId);
        if (session == null || !session.online()) {
            return null;
        }
        return session;
    }

    private long nextPacketId(ClientSession session) {
        long id = session.packetIdSequence().getAndIncrement();
        return id;
    }

    private static Qos minQos(Qos a, Qos b) {
        return a.ordinal() <= b.ordinal() ? a : b;
    }

    private static QueuedMessage toRedelivery(QueuedMessage m) {
        if (m.redelivery()) {
            return m;
        }
        return new QueuedMessage(m.packetId(), m.topic(), m.payload(), m.qos(),
                true, m.enqueuedAtMillis());
    }

    private static OperationResult reject(RejectReason reason, String detail) {
        return new OperationResult(false, reason, detail);
    }

    private static SubscriberOutcome outcome(String clientId, DeliveryDecision decision,
                                             Long packetId, RejectReason reason,
                                             String detail) {
        return new SubscriberOutcome(clientId, decision, packetId, reason, detail);
    }

    private record RouteTarget(String clientId, Qos subscribedQos) {
    }

    /** 测试与诊断用：查看会话状态快照。 */
    public SessionView sessionView(String clientId) {
        ClientSession s = registry.get(clientId);
        if (s == null) {
            return null;
        }
        Map<String, Qos> subs = new LinkedHashMap<>(s.subscriptions());
        return new SessionView(clientId, s.persistent(), s.online(), subs,
                s.pipeline().size(), s.inflight().size(),
                s.packetIdSequence().get());
    }

    /**
     * 会话诊断视图（供测试打印与断言）。
     */
    public record SessionView(String clientId, boolean persistent, boolean online,
                              Map<String, Qos> subscriptions,
                              int pipelineSize, int inflightSize, long nextPacketId) {
    }

    @SuppressWarnings("unused")
    private static final class OperationResults {
        private static OperationResult ok(String detail) {
            return new OperationResult(true, null, detail);
        }
    }
}
