package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.GatewayStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.LocalFileGatewayStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.MemoryGatewayStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.StoredState;

/**
 * 进程内 MQTT 风格消息网关：连接、订阅、发布、确认。
 *
 * <p>并发模型：所有状态变更在单把可重入锁内完成（synchronized 方法），
 * 路由判定与投递状态变更原子生效；全部操作均为内存操作，慢客户端不会阻塞其他客户端。
 *
 * <p>持久化：dataDir 非空时使用本地文件存储，进程重启后可重放恢复
 * 持久会话的订阅、离线积压、待确认消息与已确认记录；非持久会话不落盘。
 *
 * <p>所有拒绝均抛出携带可区分 {@link RejectReason} 的 {@link GatewayException}。
 */
public class Gateway implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Gateway.class);

    /** 每个会话保留的已确认消息 id 数量上限（用于识别重复确认）。 */
    private static final int ACKED_IDS_RETENTION = 10_000;

    private final GatewayConfig config;
    private final GatewayStore store;
    /** 按连接登记顺序遍历，使路由结果顺序稳定可解释。 */
    private final Map<String, Session> sessions = new LinkedHashMap<>();
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
        String dir = config.getDataDir();
        this.store = (dir == null || dir.isBlank())
                ? new MemoryGatewayStore()
                : new LocalFileGatewayStore(Path.of(dir));
        restore(this.store.replay());
        this.store.compact(takeSnapshot());
    }

    /**
     * 客户端连接。persistent=true 时保留会话、订阅与离线积压；
     * 重连时返回需补投的消息（离线积压 + 未确认的至少一次消息）。
     */
    public synchronized List<Message> connect(String clientId, boolean persistent) {
        Session session = sessions.get(clientId);
        boolean created = false;
        if (session == null || !session.isPersistent()) {
            // 非持久重连或首次连接：旧会话（若有）连同在途状态一并清除
            if (session != null) {
                dropInFlightOf(session);
            }
            session = new Session(clientId, persistent);
            sessions.put(clientId, session);
            created = true;
        }
        session.markOnline();
        log.info("[connect] client='{}' persistent={} restored={} subs={} backlog={} pending={}",
                clientId, persistent, !created, session.subscriptions().size(),
                session.backlogSize(), session.pendingAckCount());

        List<StoredState.Change> changes = new ArrayList<>();
        if (created && persistent) {
            changes.add(StoredState.Change.persistSession(clientId));
        }

        List<Message> redelivered = new ArrayList<>();
        // 1) 未确认的至少一次消息：重投（仍保留待确认，等待同一 messageId 的确认）。
        //    必须先于积压转入收集，否则新转入的积压消息会被重复计入补投列表。
        redelivered.addAll(session.pendingForRedelivery());
        // 2) 离线积压：按协商 QoS 投递，至少一次的转入待确认。
        //    注意：积压移除与待确认登记在同一个批次内原子落盘，
        //    避免崩溃发生在两者之间导致重放时丢消息。
        for (Session.BacklogEntry entry : session.drainBacklog()) {
            redelivered.add(entry.message());
            StoredState.StoredMessage sm = toStored(entry.message());
            changes.add(StoredState.Change.drainBacklog(clientId, sm));
            if (entry.qos() == QoS.AT_LEAST_ONCE) {
                session.addPendingAck(entry.message());
                inFlight.add(new InFlightKey(entry.message().id(), clientId));
                changes.add(StoredState.Change.addPending(clientId, sm));
            }
        }
        storeBatch(changes); // PERSIST_SESSION 与补投转换同一批次
        log.info("[connect] client='{}' redelivered={} ids={}",
                clientId, redelivered.size(), redelivered.stream().map(Message::id).toList());
        return redelivered;
    }

    /** 客户端断开；非持久会话被清除，持久会话保留订阅、积压与待确认。 */
    public synchronized void disconnect(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null) {
            log.info("[disconnect] client='{}' 不存在，幂等忽略", clientId);
            return; // 幂等：重复断开不算错误
        }
        if (session.isPersistent()) {
            session.markOffline();
            log.info("[disconnect] client='{}' 持久会话离线保留: subs={} backlog={} pending={}",
                    clientId, session.subscriptions().size(),
                    session.backlogSize(), session.pendingAckCount());
        } else {
            dropInFlightOf(session);
            sessions.remove(clientId);
            ackedIds.remove(clientId);
            log.info("[disconnect] client='{}' 非持久会话已清除全部状态", clientId);
        }
    }

    /** 订阅主题过滤器；非法过滤器以可区分原因拒绝。 */
    public synchronized void subscribe(String clientId, String filter, QoS qos) {
        Session session = requireSession(clientId);
        TopicFilter parsed = TopicFilter.parse(filter);
        session.addSubscription(new Subscription(clientId, parsed, qos));
        log.info("[subscribe] client='{}' filter='{}' qos={} -> 订阅数={}",
                clientId, filter, qos, session.subscriptions().size());
        if (session.isPersistent()) {
            storeBatch(List.of(StoredState.Change.addSubscription(clientId,
                    new StoredState.StoredSubscription(filter, qos.name()))));
        }
    }

    /**
     * 发布消息（默认按至少一次发布），等价于 {@link #publish(String, String, QoS)}
     * 传入 {@link QoS#AT_LEAST_ONCE}。
     */
    public synchronized List<String> publish(String topic, String payload) {
        return publish(topic, payload, QoS.AT_LEAST_ONCE);
    }

    /**
     * 按发布方 QoS 发布消息并路由到所有匹配订阅。
     * 每条投递的实际 QoS 取发布方与订阅方的较低者。
     * 返回接收该消息的客户端 id 列表（路由判定结果，按订阅登记顺序）。
     *
     * <p>背压：先按本次将新增的在途数量判定，加上现有在途后超过 maxInFlight 则整体拒绝
     * （BACKPRESSURE_LIMIT_EXCEEDED），被接受的发布绝不会让在途统计超过上限。
     * 容量：任一目标会话的离线积压达到 backlogCapacity 时整体拒绝
     * （BACKLOG_CAPACITY_EXCEEDED），消息不会部分投递，也不会静默丢弃。
     */
    public synchronized List<String> publish(String topic, String payload, QoS publisherQos) {
        TopicFilter.validateTopic(topic);

        // 路由判定（纯读取，不产生任何状态变更）：同一会话多条订阅命中只取首个匹配
        record Target(Session session, QoS subscribedQos) {
        }
        List<Target> targets = new ArrayList<>();
        for (Session session : sessions.values()) {
            for (Subscription sub : session.subscriptions()) {
                if (sub.filter().matches(topic)) {
                    targets.add(new Target(session, sub.qos()));
                    break;
                }
            }
        }

        // 协商 QoS = min(发布方, 订阅方)；统计本次将新增的在途（在线 + 协商为至少一次）
        int newInFlight = 0;
        for (Target t : targets) {
            QoS effective = QoS.lower(publisherQos, t.subscribedQos());
            if (t.session().isOnline() && effective == QoS.AT_LEAST_ONCE) {
                newInFlight++;
            }
        }
        if (inFlight.size() + newInFlight > config.getMaxInFlight()) {
            throw new GatewayException(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED,
                    "in-flight would become " + (inFlight.size() + newInFlight)
                            + " > maxInFlight " + config.getMaxInFlight()
                            + " (current " + inFlight.size() + ", new " + newInFlight + ")");
        }

        Message message = new Message(messageIds.incrementAndGet(), topic, payload);

        // 容量预检：任一离线目标积压已满则整体拒绝，避免部分投递造成结论不一致
        for (Target t : targets) {
            if (!t.session().isOnline() && t.session().backlogSize() >= config.getBacklogCapacity()) {
                throw new GatewayException(RejectReason.BACKLOG_CAPACITY_EXCEEDED,
                        "session '" + t.session().clientId() + "' backlog "
                                + t.session().backlogSize() + " >= capacity " + config.getBacklogCapacity());
            }
        }

        List<StoredState.Change> changes = new ArrayList<>();
        List<String> routed = new ArrayList<>();
        for (Target t : targets) {
            Session session = t.session();
            QoS effective = QoS.lower(publisherQos, t.subscribedQos());
            StoredState.StoredMessage sm = toStored(message);
            if (session.isOnline()) {
                if (effective == QoS.AT_LEAST_ONCE) {
                    session.addPendingAck(message);
                    inFlight.add(new InFlightKey(message.id(), session.clientId()));
                    // 仅持久会话落盘：非持久会话重启后不存在，其在途不应残留
                    if (session.isPersistent()) {
                        changes.add(StoredState.Change.addPending(session.clientId(), sm));
                    }
                }
                // 至多一次：投递即完成，无需跟踪
            } else {
                // 离线会话只可能是持久会话（非持久会话断开即删除）
                session.enqueueBacklog(message, effective);
                changes.add(StoredState.Change.enqueueBacklog(session.clientId(),
                        new StoredState.StoredBacklogEntry(sm, effective.name())));
            }
            routed.add(session.clientId());
            log.info("[route] message={} topic='{}' client='{}' online={} pubQos={} subQos={} -> effective={} {}",
                    message.id(), topic, session.clientId(), session.isOnline(),
                    publisherQos, t.subscribedQos(), effective,
                    effective == QoS.AT_LEAST_ONCE
                            ? (session.isOnline() ? "登记在途待确认" : "进入离线积压(补投时按至少一次)")
                            : (session.isOnline() ? "投递即完成" : "进入离线积压(至多一次)"));
        }
        storeBatch(changes);
        log.info("[publish] message={} topic='{}' payload='{}' pubQos={} routed={} inFlight={}",
                message.id(), topic, payload, publisherQos, routed, inFlight.size());
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
            List<StoredState.Change> changes = new ArrayList<>(2);
            if (session.isPersistent()) {
                changes.add(StoredState.Change.removePending(clientId, messageId));
                changes.add(StoredState.Change.addAckedId(clientId, messageId));
            }
            storeBatch(changes);
            log.info("[ack] client='{}' messageId={} -> 确认生效, inFlight={}",
                    clientId, messageId, inFlight.size());
            return;
        }
        if (ackedIds.getOrDefault(clientId, Set.of()).contains(messageId)) {
            log.info("[ack-reject] client='{}' messageId={} -> ACK_DUPLICATE", clientId, messageId);
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
        log.info("[ack-reject] client='{}' messageId={} -> ACK_NOT_PENDING_FOR_CLIENT ({})",
                clientId, messageId, detail);
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

    /** 查询会话订阅数（测试与观测用）。 */
    public synchronized int subscriptionCount(String clientId) {
        Session session = sessions.get(clientId);
        return session == null ? 0 : session.subscriptions().size();
    }

    /** 关闭存储（停止网关时调用，释放文件句柄）。 */
    @Override
    public synchronized void close() {
        store.close();
        log.info("[shutdown] 网关已停止，存储已关闭: dataDir={}", config.getDataDir());
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

    private void storeBatch(List<StoredState.Change> changes) {
        if (!changes.isEmpty()) {
            store.appendBatch(changes);
        }
    }

    private StoredState.StoredMessage toStored(Message m) {
        return new StoredState.StoredMessage(m.id(), m.topic(), m.payload());
    }

    // ---------------- 恢复与快照 ----------------

    private void restore(StoredState.Snapshot snapshot) {
        if (snapshot == null || snapshot.sessions().isEmpty()) {
            log.info("[restore] 无持久化状态（全新数据目录或纯内存模式）");
            return;
        }
        for (StoredState.StoredSession s : snapshot.sessions().values()) {
            Session session = Session.restore(s);
            sessions.put(s.clientId(), session);
            for (Message m : session.allMessagesForInFlight()) {
                inFlight.add(new InFlightKey(m.id(), s.clientId()));
            }
            if (!s.ackedIds().isEmpty()) {
                ackedIds.put(s.clientId(), new LinkedHashSet<>(s.ackedIds()));
            }
        }
        messageIds.set(snapshot.maxMessageId());
        log.info("[restore] 恢复 {} 个持久会话（全部离线）, maxMessageId={}, inFlight={}",
                snapshot.sessions().size(), snapshot.maxMessageId(), inFlight.size());
    }

    /** 生成当前持久状态的全量快照（仅包含持久会话）。 */
    private StoredState.Snapshot takeSnapshot() {
        Map<String, StoredState.StoredSession> out = new HashMap<>();
        for (Session session : sessions.values()) {
            if (!session.isPersistent()) {
                continue;
            }
            List<StoredState.StoredSubscription> subs = new ArrayList<>();
            for (Subscription sub : session.subscriptions()) {
                subs.add(new StoredState.StoredSubscription(sub.filter().raw(), sub.qos().name()));
            }
            List<StoredState.StoredBacklogEntry> backlog = new ArrayList<>();
            for (Session.BacklogEntry e : session.backlogEntries()) {
                backlog.add(new StoredState.StoredBacklogEntry(toStored(e.message()), e.qos().name()));
            }
            List<StoredState.StoredMessage> pending = new ArrayList<>();
            for (Message m : session.pendingMessages()) {
                pending.add(toStored(m));
            }
            out.put(session.clientId(), new StoredState.StoredSession(
                    session.clientId(), subs, backlog, pending,
                    List.copyOf(ackedIds.getOrDefault(session.clientId(), Set.of()))));
        }
        return new StoredState.Snapshot(out, messageIds.get());
    }
}
