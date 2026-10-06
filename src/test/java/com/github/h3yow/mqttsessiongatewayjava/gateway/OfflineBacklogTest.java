package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 持久会话与离线积压：重连不丢不重；积压超容量与背压触发时明确拒绝。
 */
class OfflineBacklogTest {

    private static final Logger log = LoggerFactory.getLogger(OfflineBacklogTest.class);

    @Test
    void offlineMessagesAreBackloggedAndRedeliveredOnReconnect() {
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("dev1", true);
        gateway.subscribe("dev1", "telemetry/#", QoS.AT_LEAST_ONCE);
        gateway.disconnect("dev1");

        gateway.publish("telemetry/a", "m1");
        gateway.publish("telemetry/b", "m2");
        log.info("[backlog] offline backlog={} (expect 2)", gateway.backlogSize("dev1"));
        assertEquals(2, gateway.backlogSize("dev1"));

        List<Message> redelivered = gateway.connect("dev1", true);
        log.info("[redeliver] count={} ids={} (expect积压全部补投)",
                redelivered.size(), redelivered.stream().map(Message::id).toList());
        assertEquals(2, redelivered.size());
        assertEquals(0, gateway.backlogSize("dev1"));
        // 至少一次：补投后转入待确认
        assertEquals(2, gateway.pendingAckCount("dev1"));
    }

    @Test
    void unackedMessagesAreRedeliveredOnReconnectWithoutDuplication() {
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("dev1", true);
        gateway.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.publish("t/a", "m1"); // id=1，在线投递，待确认
        gateway.disconnect("dev1");   // 未确认即掉线

        List<Message> redelivered = gateway.connect("dev1", true);
        log.info("[redeliver-unacked] ids={} (expect [1]: 未确认消息重投)",
                redelivered.stream().map(Message::id).toList());
        assertEquals(List.of(1L), redelivered.stream().map(Message::id).toList());

        gateway.ack("dev1", 1L); // 重投后确认成功
        assertEquals(0, gateway.pendingAckCount("dev1"));
        // 再次重连不应有任何补投（不重复投递）
        gateway.disconnect("dev1");
        redelivered = gateway.connect("dev1", true);
        log.info("[redeliver-after-ack] count={} (expect 0: 已确认不重复投递)", redelivered.size());
        assertTrue(redelivered.isEmpty());
    }

    @Test
    void persistentSessionSurvivesGatewayRestartEquivalent() {
        // 网关重启等价于所有会话掉线；持久会话状态保存在会话存储中，
        // 这里用同一 Gateway 上的 断开-重连 模拟（进程内会话存储即持久层）。
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("dev1", true);
        gateway.subscribe("dev1", "t/#", QoS.AT_MOST_ONCE);
        gateway.disconnect("dev1");
        gateway.publish("t/a", "m1");

        List<Message> redelivered = gateway.connect("dev1", true);
        log.info("[restart] qos0 redelivered={} (expect 1: 至多一次离线也积压但不需确认)",
                redelivered.size());
        assertEquals(1, redelivered.size());
        assertEquals(0, gateway.pendingAckCount("dev1")); // 至多一次不进入待确认
    }

    @Test
    void backlogCapacityExceededRejectsPublish() {
        GatewayConfig config = new GatewayConfig();
        config.setBacklogCapacity(2);
        Gateway gateway = new Gateway(config);
        gateway.connect("dev1", true);
        gateway.subscribe("dev1", "t/#", QoS.AT_MOST_ONCE);
        gateway.disconnect("dev1");

        gateway.publish("t/a", "m1");
        gateway.publish("t/b", "m2");
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.publish("t/c", "m3"));
        log.info("[capacity] backlog={} capacity=2 publish#3 -> reason={} (不静默丢弃)",
                gateway.backlogSize("dev1"), ex.reason());
        assertEquals(RejectReason.BACKLOG_CAPACITY_EXCEEDED, ex.reason());
        assertEquals(2, gateway.backlogSize("dev1")); // 第 3 条未进入积压
    }

    @Test
    void backpressureRejectsPublishWhenInFlightFull() {
        GatewayConfig config = new GatewayConfig();
        config.setMaxInFlight(2);
        Gateway gateway = new Gateway(config);
        gateway.connect("c1", false);
        gateway.connect("c2", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.subscribe("c2", "t/#", QoS.AT_LEAST_ONCE);

        // 本次发布将新增 2 个在途，恰好等于上限：接受，在途严格等于上限
        gateway.publish("t/a", "m1");
        assertEquals(2, gateway.inFlightCount());
        // 再发一条将新增 2 个在途（2+2=4 > 2）：产生任何投递之前整体拒绝
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.publish("t/b", "m2"));
        log.info("[backpressure] inFlight={}/2 本次新增=2 -> reason={}（整批拒绝，无部分投递）",
                gateway.inFlightCount(), ex.reason());
        assertEquals(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED, ex.reason());
        assertEquals(2, gateway.inFlightCount()); // 被拒发布不产生任何在途
        assertEquals(1, gateway.pendingAckCount("c1")); // 无部分投递：c1 仍只有 m1
        assertEquals(1, gateway.pendingAckCount("c2"));

        gateway.ack("c1", 1L); // 确认后释放额度（在途 2 -> 1）
        // 2 个目标仍会新增 2（1+2=3 > 2）：依旧拒绝，不允许投一个留一个
        ex = assertThrows(GatewayException.class, () -> gateway.publish("t/b", "m2"));
        assertEquals(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED, ex.reason());
        log.info("[backpressure] inFlight={}/2 ack 后仍不足 2 个额度 -> reason={}（不投一半）",
                gateway.inFlightCount(), ex.reason());

        gateway.ack("c2", 1L); // 在途 1 -> 0，额度恢复
        gateway.publish("t/b", "m2"); // 新增 2，恰好到上限，接受
        log.info("[backpressure] 额度恢复后发布 inFlight={} (expect 2，严格不超上限)",
                gateway.inFlightCount());
        assertEquals(2, gateway.inFlightCount());
    }
}
