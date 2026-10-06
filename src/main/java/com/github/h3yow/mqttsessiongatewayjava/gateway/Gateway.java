package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进程内 MQTT 风格消息网关：连接、订阅、发布、确认。
 *
 * <p>并发模型：所有状态变更在单把可重入锁内完成（synchronized 方法），
 * 路由判定与投递状态变更原子生效，因此并发发布/订阅/确认下，
 * 同一主题的路由结果一致；全部操作均为内存操作，慢客户端不会阻塞其他客户端。
 *
 * <p>重启恢复：当配置了本地数据目录（{@code gateway.data-dir}）时，每次状态变更后
 * 全量快照原子落盘；新进程指向同一目录构造网关时，持久会话的订阅、离线积压、
 * 未确认消息与已确认记录原样恢复（恢复后一律为离线，等待重连）。
 * 非持久会话从不落盘。未配置数据目录时为纯内存模式。
 *
 * <p>所有拒绝均抛出携带可区分 {@link RejectReason} 的 {@link GatewayException}。
 */
public class Gateway {

    private static final Logger log = LoggerFactory.getLogger(Gateway.class);

    /** 每个会话保留的已确认消息 id 数量上限（用于识别重复确认）。 */
    private static final int ACKED_IDS_RETENTION = 10_000;

    private final GatewayConfig config;
    private final GatewayStateStore stateStore;
    private final Map<String, Session> sessions = new HashMap<>();
    private final AtomicLong messageIds = new AtomicLong();

    /** 在途消息键：一次投递 = (消息 id, 接收客户端)。 */
    private record InFlightKey(long messageId, String clientId) {
    }

    /** 在途投递集合（背压统计与归属判定依据）。 */
    private final Set<InFlightKey> inFlight = new HashSet<>();

    /** 每个会话最近已确认的消息 id（有界，用于区分重复确认）。 */
    private final Map<String, Set<Long>> ackedIds = new HashMap<>();

    public Gateway(GatewayConfig config) {
        this.config = config;
        this.stateStore = createStateStore(config);
        restore();
    }

    private static GatewayStateStore createStateStore(GatewayConfig config) {
        String dir = config.getDataDir();
        if (dir == null || dir.isBlank()) {
            return null; // 纯内存模式
        }
        return new LocalFileStateStore(Path.of(dir));
    }

    /** 启动时从本地快照恢复持久会话；在构造（未发布、无锁竞争）阶段执行。 */
    private void restore() {
        if (stateStore == null) {
            log.info("[gateway] 以纯内存模式启动（未配置 gateway.data-dir）");
            return;
        }
        Optional<GatewaySnapshot> loaded = stateStore.load();
        if (loaded.isEmpty()) {
            log.info("[gateway] 无历史快照，持久化模式冷启动 dataDir='{}'", config.getDataDir());
            return;
        }
        GatewaySnapshot snapshot = loaded.get();
        for (GatewaySnapshot.SessionSnapshot ss : snapshot.sessions()) {
            Session session = Session.restore(ss);
            sessions.put(session.clientId(), session);
            // 恢复在途：未确认消息重启后仍占在途额度，直到被确认
            for (Message m : ss.pendingAck()) {
                inFlight.add(new InFlightKey(m.id(), session.clientId()));
            }
            if (!ss.ackedIds().isEmpty()) {
                ackedIds.put(session.clientId(), new LinkedHashSet<>(ss.ackedIds()));
            }
            log.info("[restore] client='{}' subscriptions={} backlog={} pendingAck={} ackedIds={} -> 离线待重连",
                    session.clientId(), ss.subscriptions().size(), ss.backlog().size(),
                    ss.pendingAck().size(), ss.ackedIds().size());
        }
        messageIds.set(snapshot.lastMessageId());
        log.info("[gateway] 快照恢复完成 sessions={} lastMessageId={} inFlight={}",
                sessions.size(), snapshot.lastMessageId(), inFlight.size());
    }

    /**
     * 将持久会话状态原子落盘；纯内存模式下为空操作。
     * 必须在持锁（synchronized）状态下调用，保证快照与内存状态一致。
     */
    private void persist() {
        if (stateStore == null) {
            return;
        }
        List<GatewaySnapshot.SessionSnapshot> snapshotSessions = new ArrayList<>();
        for (Session session : sessions.values()) {
            if (!session.isPersistent()) {
                continue; // 非持久会话从不落盘
            }
            snapshotSessions.add(session.toSnapshot(
                    List.copyOf(ackedIds.getOrDefault(session.clientId(), Set.of()))));
        }
        stateStore.save(new GatewaySnapshot(messageIds.get(), snapshotSessions));
    }

    /**
     * 客户端连接。persistent=true 时保留会话、订阅与离线积压；
     * 重连时返回需补投的消息（离线积压 + 未确认的至少一次消息）。
     * 非持久重连会丢弃同名的旧持久/非持久会话。
     */
    public synchronized List<Message> connect(String clientId, boolean persistent) {
        Session session = sessions.get(clientId);
        if (session == null || !session.isPersistent()) {
            // 非持久重连或首次连接：旧会话（若有）连同在途状态一并清除
            if (session != null) {
                dropInFlightOf(session);
            }
            session = new Session(clientId, persistent);
            sessions.put(clientId, session);
            log.info("[connect] client='{}' persistent={} 新会话", clientId, persistent);
        } else {
            log.info("[connect] client='{}' persistent=true 复用持久会话 backlog={} pendingAck={}",
                    clientId, session.backlogSize(), session.pendingAckCount());
        }
        session.markOnline();

        List<Message> redelivered = new ArrayList<>();
        // 1) 未确认的至少一次消息：重投（仍保留待确认，等待同一 messageId 的确认）。
        //    必须先于积压转入收集，否则新转入的积压消息会被重复计入补投列表。
        int pendingCount = session.pendingAckCount();
        redelivered.addAll(session.pendingForRedelivery());
        // 2) 离线积压：按协商后的 QoS 投递，至少一次的转入待确认
        int backlogCount = session.backlogSize();
        for (Session.BacklogEntry entry : session.drainBacklog()) {
            redelivered.add(entry.message());
            if (entry.qos() == QoS.AT_LEAST_ONCE) {
                session.addPendingAck(entry.message());
                inFlight.add(new InFlightKey(entry.message().id(), clientId));
            }
        }
        log.info("[connect] client='{}' 上线 redelivered={} 判定依据=未确认待重投{}条+离线积压{}条",
                clientId, redelivered.size(), pendingCount, backlogCount);
        persist();
        return redelivered;
    }

    /** 客户端断开；非持久会话被清除，持久会话保留订阅、积压与待确认。 */
    public synchronized void disconnect(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null) {
            log.info("[disconnect] client='{}' 无会话（幂等）", clientId);
            return; // 幂等：重复断开不算错误
        }
        if (session.isPersistent()) {
            session.markOffline();
            log.info("[disconnect] client='{}' 持久会话离线保留 backlog={} pendingAck={}",
                    clientId, session.backlogSize(), session.pendingAckCount());
        } else {
            dropInFlightOf(session);
            sessions.remove(clientId);
            ackedIds.remove(clientId);
            log.info("[disconnect] client='{}' 非持久会话已彻底清除", clientId);
        }
        persist();
    }

    /** 订阅主题过滤器；非法过滤器以可区分原因拒绝。 */
    public synchronized void subscribe(String clientId, String filter, QoS qos) {
        Session session = requireSession(clientId);
        TopicFilter parsed = TopicFilter.parse(filter);
        session.addSubscription(new Subscription(clientId, parsed, qos));
        log.info("[subscribe] client='{}' filter='{}' subQos={} online={}",
                clientId, filter, qos, session.isOnline());
        persist();
    }

    /**
     * 以默认至少一次的发布 QoS 发布消息（向后兼容）。
     */
    public synchronized List<String> publish(String topic, String payload) {
        return publish(topic, payload, QoS.AT_LEAST_ONCE);
    }

    /**
     * 发布消息并路由到所有匹配订阅。{@code publishQos} 为发布方要求的质量等级，
     * 每条投递按 {@code min(发布方 QoS, 订阅方 QoS)} 协商后的等级处理。
     * 返回接收该消息的客户端 id 列表（路由判定结果，按订阅登记顺序）。
     *
     * <p>背压：若本次投递将使全网关在途数超过 maxInFlight，则在产生任何投递之前
     * 整体拒绝（BACKPRESSURE_LIMIT_EXCEEDED）。
     * 容量：任一离线目标会话的积压已满时，在产生任何投递之前整体拒绝
     * （BACKLOG_CAPACITY_EXCEEDED）。两类拒绝都不会投一半留一半，也不会静默丢弃。
     */
    public synchronized List<String> publish(String topic, String payload, QoS publishQos) {
        TopicFilter.validateTopic(topic);

        // 路由判定：收集所有匹配订阅（判定与投递在同一临界区内，保证一致性）
        record Target(Session session, QoS subQos, QoS effectiveQos) {
        }
        List<Target> targets = new ArrayList<>();
        for (Session session : sessions.values()) {
            for (Subscription sub : session.subscriptions()) {
                if (sub.filter().matches(topic)) {
                    QoS effective = QoS.negotiate(publishQos, sub.qos());
                    targets.add(new Target(session, sub.qos(), effective));
                    break; // 同一会话多条订阅匹配只投递一次，取首个匹配的 QoS
                }
            }
        }

        // 容量预检（在分配消息 id、产生任何投递之前）：
        // 1) 离线目标积压：任一已满则整体拒绝
        for (Target t : targets) {
            if (!t.session().isOnline() && t.session().backlogSize() >= config.getBacklogCapacity()) {
                log.warn("[publish-reject] topic='{}' pubQos={} 原因=BACKLOG_CAPACITY_EXCEEDED"
                                + " session='{}' backlog={}/{} 判定=预检失败，未产生任何投递",
                        topic, publishQos, t.session().clientId(),
                        t.session().backlogSize(), config.getBacklogCapacity());
                throw new GatewayException(RejectReason.BACKLOG_CAPACITY_EXCEEDED,
                        "session '" + t.session().clientId() + "' backlog "
                                + t.session().backlogSize() + " >= capacity " + config.getBacklogCapacity());
            }
        }
        // 2) 在途额度：本次将新增的至少一次在线投递数 + 当前在途不得超过上限
        int inFlightDelta = 0;
        for (Target t : targets) {
            if (t.session().isOnline() && t.effectiveQos() == QoS.AT_LEAST_ONCE) {
                inFlightDelta++;
            }
        }
        if (inFlight.size() + inFlightDelta > config.getMaxInFlight()) {
            log.warn("[publish-reject] topic='{}' pubQos={} 原因=BACKPRESSURE_LIMIT_EXCEEDED"
                            + " inFlight={}+本次新增{} > maxInFlight={} 判定=预检失败，未产生任何投递",
                    topic, publishQos, inFlight.size(), inFlightDelta, config.getMaxInFlight());
            throw new GatewayException(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED,
                    "in-flight " + inFlight.size() + " + " + inFlightDelta
                            + " > maxInFlight " + config.getMaxInFlight());
        }

        // 预检全部通过后才分配消息 id：被拒绝的发布不消耗 id
        Message message = new Message(messageIds.incrementAndGet(), topic, payload);

        List<String> routed = new ArrayList<>();
        for (Target t : targets) {
            Session session = t.session();
            String decision;
            if (session.isOnline()) {
                if (t.effectiveQos() == QoS.AT_LEAST_ONCE) {
                    session.addPendingAck(message);
                    inFlight.add(new InFlightKey(message.id(), session.clientId()));
                    decision = "在线+至少一次(待确认)";
                } else {
                    decision = "在线+至多一次(不跟踪)";
                }
            } else {
                session.enqueueBacklog(message, t.effectiveQos());
                decision = "离线+入积压(" + t.effectiveQos() + ")";
            }
            routed.add(session.clientId());
            log.info("[route] messageId={} topic='{}' client='{}' pubQos={} subQos={}"
                            + " -> effectiveQos={} 判定={}",
                    message.id(), topic, session.clientId(), publishQos, t.subQos(),
                    t.effectiveQos(), decision);
        }
        log.info("[publish-accept] messageId={} topic='{}' pubQos={} targets={} inFlight={}/{}",
                message.id(), topic, publishQos, routed, inFlight.size(), config.getMaxInFlight());
        persist();
        return routed;
    }

    /**
     * 确认一条至少一次消息。
     * 重复确认（ACK_DUPLICATE）与非对应订阅者的确认
     * （ACK_NOT_PENDING_FOR_CLIENT）以可区分原因拒绝。
     */
    public synchronized void ack(String clientId, long messageId) {
        Session session = requireSession(clientId);
        if (session.hasPending(messageId)) {
            session.removePending(messageId);
            inFlight.remove(new InFlightKey(messageId, clientId));
            rememberAcked(clientId, messageId);
            log.info("[ack-ok] client='{}' messageId={} 在途释放 -> inFlight={}",
                    clientId, messageId, inFlight.size());
            persist();
            return;
        }
        if (ackedIds.getOrDefault(clientId, Set.of()).contains(messageId)) {
            log.warn("[ack-reject] client='{}' messageId={} 原因=ACK_DUPLICATE",
                    clientId, messageId);
            throw new GatewayException(RejectReason.ACK_DUPLICATE,
                    "message " + messageId + " already acknowledged by '" + clientId + "'");
        }
        String owner = null;
        for (InFlightKey key : inFlight) {
            if (key.messageId() == messageId) {
                owner = key.clientId();
                break;
            }
        }
        String detail = owner != null
                ? "message " + messageId + " is pending for '" + owner + "', not '" + clientId + "'"
                : "message " + messageId + " is not pending for '" + clientId + "'";
        log.warn("[ack-reject] client='{}' messageId={} 原因=ACK_NOT_PENDING_FOR_CLIENT owner={}",
                clientId, messageId, owner);
        throw new GatewayException(RejectReason.ACK_NOT_PENDING_FOR_CLIENT, detail);
    }

    /** 查询会话积压大小（测试与观测用）。 */
    public synchronized int backlogSize(String clientId) {
        Session session = sessions.get(clientId);
        return session == null ? 0 : session.backlogSize();
    }

    /** 查询会话待确认数量（测试与观测用）。 */
    public synchronized int pendingAckCount(String clientId) {
        Session session = sessions.get(clientId);
        return session == null ? 0 : session.pendingAckCount();
    }

    /** 当前全网关在途消息数（背压观测用）。 */
    public synchronized int inFlightCount() {
        return inFlight.size();
    }

    private Session requireSession(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null) {
            throw new GatewayException(RejectReason.SESSION_NOT_FOUND,
                    "no session for client '" + clientId + "'");
        }
        return session;
    }

    private void dropInFlightOf(Session session) {
        for (Message m : session.pendingForRedelivery()) {
            inFlight.remove(new InFlightKey(m.id(), session.clientId()));
        }
    }    private void rememberAcked(String clientId, long messageId) {
        Set<Long> ids = ackedIds.computeIfAbsent(clientId, k -> new LinkedHashSet<>());
        ids.add(messageId);
        if (ids.size() > ACKED_IDS_RETENTION) {
            ids.remove(ids.iterator().next()); // 淘汰最旧记录，防止无限增长
        }
    }
}
