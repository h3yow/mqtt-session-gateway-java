package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内 MQTT 风格消息网关：连接、订阅、发布、确认。
 *
 * <p>并发模型：所有状态变更在单把可重入锁内完成（synchronized 方法），
 * 路由判定与投递状态变更原子生效，因此并发发布/订阅/确认下，
 * 同一主题的路由结果一致；全部操作均为内存操作，慢客户端不会阻塞其他客户端。
 *
 * <p>所有拒绝均抛出携带可区分 {@link RejectReason} 的 {@link GatewayException}。
 */
public class Gateway {

    /** 每个会话保留的已确认消息 id 数量上限（用于识别重复确认）。 */
    private static final int ACKED_IDS_RETENTION = 10_000;

    private final GatewayConfig config;
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
    }

    /**
     * 客户端连接。persistent=true 时保留会话、订阅与离线积压；
     * 重连时返回需补投的消息（离线积压 + 未确认的至少一次消息）。
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
        }
        session.markOnline();

        List<Message> redelivered = new ArrayList<>();
        // 1) 未确认的至少一次消息：重投（仍保留待确认，等待同一 messageId 的确认）。
        //    必须先于积压转入收集，否则新转入的积压消息会被重复计入补投列表。
        redelivered.addAll(session.pendingForRedelivery());
        // 2) 离线积压：按订阅 QoS 投递，至少一次的转入待确认
        for (Session.BacklogEntry entry : session.drainBacklog()) {
            redelivered.add(entry.message());
            if (entry.qos() == QoS.AT_LEAST_ONCE) {
                session.addPendingAck(entry.message());
                inFlight.add(new InFlightKey(entry.message().id(), clientId));
            }
        }
        return redelivered;
    }

    /** 客户端断开；非持久会话被清除，持久会话保留订阅、积压与待确认。 */
    public synchronized void disconnect(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null) {
            return; // 幂等：重复断开不算错误
        }
        if (session.isPersistent()) {
            session.markOffline();
        } else {
            dropInFlightOf(session);
            sessions.remove(clientId);
            ackedIds.remove(clientId);
        }
    }

    /** 订阅主题过滤器；非法过滤器以可区分原因拒绝。 */
    public synchronized void subscribe(String clientId, String filter, QoS qos) {
        Session session = requireSession(clientId);
        TopicFilter parsed = TopicFilter.parse(filter);
        session.addSubscription(new Subscription(clientId, parsed, qos));
    }

    /**
     * 发布消息并路由到所有匹配订阅。
     * 返回接收该消息的客户端 id 列表（路由判定结果，按订阅登记顺序）。
     *
     * <p>背压：全网关在途消息达到 maxInFlight 时拒绝发布（BACKPRESSURE_LIMIT_EXCEEDED）。
     * 容量：任一目标会话的离线积压达到 backlogCapacity 时拒绝发布
     * （BACKLOG_CAPACITY_EXCEEDED），消息不会静默丢弃。
     */
    public synchronized List<String> publish(String topic, String payload) {
        TopicFilter.validateTopic(topic);
        if (inFlight.size() >= config.getMaxInFlight()) {
            throw new GatewayException(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED,
                    "in-flight " + inFlight.size() + " >= maxInFlight " + config.getMaxInFlight());
        }
        Message message = new Message(messageIds.incrementAndGet(), topic, payload);

        // 路由判定：收集所有匹配订阅（判定与投递在同一临界区内，保证一致性）
        record Target(Session session, QoS qos) {
        }
        List<Target> targets = new ArrayList<>();
        for (Session session : sessions.values()) {
            for (Subscription sub : session.subscriptions()) {
                if (sub.filter().matches(topic)) {
                    targets.add(new Target(session, sub.qos()));
                    break; // 同一会话多条订阅匹配只投递一次，取首个匹配的 QoS
                }
            }
        }

        // 容量预检：任一离线目标积压已满则整体拒绝，避免部分投递造成结论不一致
        for (Target t : targets) {
            if (!t.session().isOnline() && t.session().backlogSize() >= config.getBacklogCapacity()) {
                throw new GatewayException(RejectReason.BACKLOG_CAPACITY_EXCEEDED,
                        "session '" + t.session().clientId() + "' backlog "
                                + t.session().backlogSize() + " >= capacity " + config.getBacklogCapacity());
            }
        }

        List<String> routed = new ArrayList<>();
        for (Target t : targets) {
            Session session = t.session();
            if (session.isOnline()) {
                if (t.qos() == QoS.AT_LEAST_ONCE) {
                    session.addPendingAck(message);
                    inFlight.add(new InFlightKey(message.id(), session.clientId()));
                }
                // 至多一次：投递即完成，无需跟踪
            } else {
                session.enqueueBacklog(message, t.qos());
            }
            routed.add(session.clientId());
        }
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
            return;
        }
        if (ackedIds.getOrDefault(clientId, Set.of()).contains(messageId)) {
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
    }

    private void rememberAcked(String clientId, long messageId) {
        Set<Long> ids = ackedIds.computeIfAbsent(clientId, k -> new LinkedHashSet<>());
        ids.add(messageId);
        if (ids.size() > ACKED_IDS_RETENTION) {
            ids.remove(ids.iterator().next()); // 淘汰最旧记录，防止无限增长
        }
    }
}
