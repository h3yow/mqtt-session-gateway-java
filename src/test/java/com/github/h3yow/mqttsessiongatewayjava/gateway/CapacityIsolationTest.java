package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 容量与背压严格性：任何被接受的发布都不能让实际积压/在途统计超过各自上限；
 * 超限发布在产生任何投递之前整体拒绝（不投一半留一半、不静默丢弃）；
 * 一个设备占满自己的额度不连累其他设备的发布、订阅与确认。
 */
class CapacityIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityIsolationTest.class);

    @Test
    void backlogNeverExceedsCapacityAndRejectionIsAllOrNothing() {
        GatewayConfig config = new GatewayConfig();
        config.setBacklogCapacity(2);
        Gateway gw = new Gateway(config);
        // 两个离线持久会话都匹配同一主题
        gw.connect("o1", true);
        gw.connect("o2", true);
        gw.subscribe("o1", "t/#", QoS.AT_MOST_ONCE);
        gw.subscribe("o2", "t/#", QoS.AT_MOST_ONCE);
        gw.disconnect("o1");
        gw.disconnect("o2");
        // 一个在线会话也匹配，用于验证"整体拒绝"时在线方也收不到
        gw.connect("online", false);
        gw.subscribe("online", "t/#", QoS.AT_MOST_ONCE);

        gw.publish("t/a", "1"); // o1、o2 积压各 1
        gw.publish("t/b", "2"); // o1、o2 积压各 2（满）
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gw.publish("t/c", "3"));
        log.info("[strict-backlog] o1={} o2={} capacity=2 第3条发布 -> reason={} (整体拒绝)",
                gw.backlogSize("o1"), gw.backlogSize("o2"), ex.reason());
        assertEquals(RejectReason.BACKLOG_CAPACITY_EXCEEDED, ex.reason());
        // 没有任何一个目标被投递：离线方积压不增长，在线方也没有待确认/其他副作用
        assertEquals(2, gw.backlogSize("o1"));
        assertEquals(2, gw.backlogSize("o2"));
        // 第 3 条被整体拒绝，路由结果根本不会返回
    }

    @Test
    void inFlightNeverExceedsLimitEvenWhenOneTargetWouldFit() {
        GatewayConfig config = new GatewayConfig();
        config.setMaxInFlight(3);
        Gateway gw = new Gateway(config);
        gw.connect("a", false);
        gw.connect("b", false);
        gw.connect("c", false);
        gw.subscribe("a", "t/#", QoS.AT_LEAST_ONCE);
        gw.subscribe("b", "t/#", QoS.AT_LEAST_ONCE);
        gw.subscribe("c", "other/#", QoS.AT_LEAST_ONCE); // c 暂不匹配 t

        gw.publish("t/a", "1"); // 投给 a、b -> 在途 2/3，c 还有 1 个额度
        // c 现在开始匹配 t；再发一条 t 本应投给 a、b、c 三个目标（新增 3 在途 -> 5 > 3）。
        // 即使 c 单独看放得下，也必须整体拒绝，不能只投 c。
        gw.subscribe("c", "t/#", QoS.AT_LEAST_ONCE);
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gw.publish("t/b", "2"));
        log.info("[strict-inflight] current={} wouldAdd=3 max=3 -> reason={} (不允许只投给有额度的c)",
                gw.inFlightCount(), ex.reason());
        assertEquals(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED, ex.reason());
        assertEquals(2, gw.inFlightCount(), "实际在途统计绝不超过上限");
        assertEquals(0, gw.pendingAckCount("c"), "整体拒绝：即使 c 有额度也不能投一半");

        // 释放两个额度后（在途 0），同样的发布完整投递给 a、b、c -> 在途 3，恰好等于上限
        gw.ack("a", 1L);
        gw.ack("b", 1L);
        List<String> routed = gw.publish("t/b", "2");
        log.info("[strict-inflight] 释放后发布 routed={} inFlight={} (expect 3: 恰好等于上限可接受)",
                routed, gw.inFlightCount());
        assertEquals(3, routed.size());
        assertTrue(routed.containsAll(List.of("a", "b", "c")));
        assertEquals(3, gw.inFlightCount());

        // 在途已满（3/3）时任何 QoS1 发布都被拒绝
        ex = assertThrows(GatewayException.class, () -> gw.publish("t/c", "3"));
        assertEquals(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED, ex.reason());
        assertEquals(3, gw.inFlightCount());
    }

    @Test
    void oneClientFillingBacklogDoesNotBlockOthers() {
        GatewayConfig config = new GatewayConfig();
        config.setBacklogCapacity(1);
        config.setMaxInFlight(20);
        Gateway gw = new Gateway(config);
        // slow：离线持久，积压 1/1 立刻打满
        gw.connect("slow", true);
        gw.subscribe("slow", "busy/#", QoS.AT_LEAST_ONCE);
        gw.disconnect("slow");
        // 两个正常设备：一个在线、一个离线（订阅不同主题）
        gw.connect("fastOnline", false);
        gw.subscribe("fastOnline", "other/#", QoS.AT_LEAST_ONCE);
        gw.connect("fastOffline", true);
        gw.subscribe("fastOffline", "other/#", QoS.AT_LEAST_ONCE);
        gw.disconnect("fastOffline");

        gw.publish("busy/a", "m1"); // slow 积压满
        assertEquals(1, gw.backlogSize("slow"));

        // slow 主题的后续发布被明确拒绝，而不是静默丢弃
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gw.publish("busy/b", "m2"));
        log.info("[isolation] slow 积压满 -> busy 主题 reason={}", ex.reason());
        assertEquals(RejectReason.BACKLOG_CAPACITY_EXCEEDED, ex.reason());

        // 其他设备的发布（在线投递、离线积压）照常工作（路由顺序不做保证，断言集合）
        List<String> routed = gw.publish("other/x", "ok1");
        log.info("[isolation] other 主题 routed={} slowBacklog={} fastOfflineBacklog={}",
                routed, gw.backlogSize("slow"), gw.backlogSize("fastOffline"));
        assertEquals(2, routed.size());
        assertTrue(routed.contains("fastOnline"));
        assertTrue(routed.contains("fastOffline"));
        assertEquals(1, gw.backlogSize("fastOffline"));
        assertEquals(1, gw.backlogSize("slow"), "slow 的额度状态未被他人操作改变");

        // 其他设备的确认照常；slow 的拒绝不阻塞任何操作
        gw.ack("fastOnline", 3L);
        assertEquals(0, gw.pendingAckCount("fastOnline"));

        // slow 重连消化积压、释放自己的额度后，又能继续接收，全程不影响别人
        List<Message> redelivered = gw.connect("slow", true);
        assertEquals(1, redelivered.size());
        gw.ack("slow", redelivered.get(0).id());
        gw.disconnect("slow");
        List<String> routedAfter = gw.publish("busy/c", "m3");
        log.info("[isolation] slow 消化恢复后 busy 发布 routed={} backlog={}",
                routedAfter, gw.backlogSize("slow"));
        assertEquals(List.of("slow"), routedAfter);
        assertEquals(1, gw.backlogSize("slow"));
    }
}
