package com.github.h3yow.mqttsessiongatewayjava.tests;

import com.github.h3yow.mqttsessiongatewayjava.gateway.MqttGateway;
import com.github.h3yow.mqttsessiongatewayjava.gateway.config.GatewayProperties;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.DeliveryDecision;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.OperationResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishRequest;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 投递与确认语义：
 * 有效QoS降级、重叠订阅去重、QoS0 拒绝确认、重复 ACK、
 * 非对应订阅者的 ACK、未投递 packetId 等失败原因必须可区分。
 */
class DeliveryAckSemanticsTest {

    private static final Logger log = LoggerFactory.getLogger(DeliveryAckSemanticsTest.class);

    @TempDir
    Path dataDir;

    private GatewayProperties props;
    private RecordingDeliveryHandler handler;
    private MqttGateway gw;

    @BeforeEach
    void setUp() {
        props = GatewayTestSupport.properties(dataDir, 1000);
        handler = new RecordingDeliveryHandler();
        gw = GatewayTestSupport.gateway(props, handler);
    }

    @AfterEach
    void tearDown() {
        gw.shutdown();
    }

    @Test
    void qosDowngradeAndOverlappingFiltersDeliverOnce() {
        gw.connect("dev-a", false);
        gw.subscribe("dev-a", "a/+/c", Qos.AT_MOST_ONCE);
        gw.subscribe("dev-a", "a/#", Qos.AT_LEAST_ONCE);

        PublishRequest req = PublishRequest.of("a/b/c", Qos.AT_LEAST_ONCE, "hi");
        PublishResult result = gw.publish(req);
        GatewayTestSupport.logPublish("重叠订阅", req, result);

        assertEquals(1, result.outcomes().size(), "同一客户端重叠过滤器只投递一次");
        // 取最高订阅 QoS(1) 与发布 QoS(1) 的 min => 1
        assertEquals(DeliveryDecision.DELIVERED, result.outcomes().get(0).decision());

        // 订阅降级：订阅 QoS0，发布 QoS1 => 有效 QoS0
        gw.connect("dev-b", false);
        gw.subscribe("dev-b", "#", Qos.AT_MOST_ONCE);
        PublishRequest req2 = PublishRequest.of("x/y", Qos.AT_LEAST_ONCE, "hi2");
        PublishResult r2 = gw.publish(req2);
        GatewayTestSupport.logPublish("QoS降级", req2, r2);
        assertTrue(r2.outcomes().get(0).detail().contains("有效QoS=AT_MOST_ONCE"));
    }

    @Test
    void qos0AckIsRejected() throws Exception {
        gw.connect("dev-a", false);
        gw.subscribe("dev-a", "t/0", Qos.AT_MOST_ONCE);
        PublishResult r = gw.publish(PublishRequest.of("t/0", Qos.AT_MOST_ONCE, "m"));
        Long pid = r.outcomes().get(0).packetId();
        assertNotNull(pid);
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(2),
                () -> !handler.of("dev-a").isEmpty()));
        log.info("输入: acknowledge(client=dev-a, packetId={})，会话状态={}",
                pid, gw.sessionView("dev-a"));
        OperationResult ack = gw.acknowledge("dev-a", pid);
        log.info("判定依据: QoS0 无确认语义 => accepted={} reason={}",
                ack.accepted(), ack.reason());
        assertFalse(ack.accepted());
        assertEquals(RejectReason.QOS0_DOES_NOT_ACK, ack.reason());
    }

    @Test
    void qos1AckThenDuplicateAckRejected() {
        gw.connect("dev-a", false);
        gw.subscribe("dev-a", "t/1", Qos.AT_LEAST_ONCE);
        PublishResult r = gw.publish(PublishRequest.of("t/1", Qos.AT_LEAST_ONCE, "m"));
        long pid = r.outcomes().get(0).packetId();
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(2),
                () -> gw.sessionView("dev-a").inflightSize() == 1));
        GatewayTestSupport.logView("首次确认前", gw, "dev-a");

        OperationResult first = gw.acknowledge("dev-a", pid);
        log.info("第一次 ACK: accepted={} detail='{}'", first.accepted(), first.detail());
        assertTrue(first.accepted());
        GatewayTestSupport.logView("首次确认后", gw, "dev-a");

        OperationResult dup = gw.acknowledge("dev-a", pid);
        log.info("重复 ACK 判定: accepted={} reason={} detail='{}'",
                dup.accepted(), dup.reason(), dup.detail());
        assertFalse(dup.accepted());
        assertEquals(RejectReason.DUPLICATE_ACK, dup.reason());
    }

    @Test
    void ackFromWrongClientAndUnknownPacketAreRejectedWithDistinctReasons() {
        gw.connect("dev-a", false);
        gw.connect("dev-b", false);
        gw.subscribe("dev-a", "t/a", Qos.AT_LEAST_ONCE);
        PublishResult r = gw.publish(PublishRequest.of("t/a", Qos.AT_LEAST_ONCE, "m"));
        long pid = r.outcomes().get(0).packetId();
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(2),
                () -> gw.sessionView("dev-a").inflightSize() == 1));

        // 非对应订阅者确认
        OperationResult wrongClient = gw.acknowledge("dev-b", pid);
        log.info("非订阅者确认: client=dev-b packetId={} => reason={} detail='{}'",
                pid, wrongClient.reason(), wrongClient.detail());
        assertFalse(wrongClient.accepted());
        assertEquals(RejectReason.PACKET_NOT_IN_FLIGHT, wrongClient.reason());

        // 从未投递过的 packetId
        OperationResult unknown = gw.acknowledge("dev-a", 99999);
        log.info("未知 packetId 确认: client=dev-a packetId=99999 => reason={} detail='{}'",
                unknown.reason(), unknown.detail());
        assertFalse(unknown.accepted());
        assertEquals(RejectReason.PACKET_NOT_IN_FLIGHT, unknown.reason());

        // 不存在的客户端
        OperationResult ghost = gw.acknowledge("ghost", pid);
        assertEquals(RejectReason.UNKNOWN_CLIENT, ghost.reason());

        // dev-a 自己确认仍应成功（未被错误确认影响）
        OperationResult ok = gw.acknowledge("dev-a", pid);
        assertTrue(ok.accepted());
    }

    @Test
    void invalidSubscriptionReasonsAreDistinguishable() {
        gw.connect("dev-a", false);
        assertEquals(RejectReason.HASH_NOT_AT_END,
                gw.subscribe("dev-a", "a/#/b", Qos.AT_LEAST_ONCE).reason());
        assertEquals(RejectReason.HASH_NOT_ALONE_IN_LEVEL,
                gw.subscribe("dev-a", "a/b#", Qos.AT_LEAST_ONCE).reason());
        assertEquals(RejectReason.ILLEGAL_LEVEL_CHARACTER,
                gw.subscribe("dev-a", "a/+x", Qos.AT_LEAST_ONCE).reason());
        OperationResult offline = gw.subscribe("nobody", "x", Qos.AT_LEAST_ONCE);
        assertEquals(RejectReason.UNKNOWN_CLIENT, offline.reason());
        log.info("非法订阅各原因均已区分");
    }

    @Test
    void messageNeverRoutesToNonMatchingSubscription() {
        gw.connect("dev-a", false);
        gw.subscribe("dev-a", "sport/tennis/+", Qos.AT_LEAST_ONCE);
        gw.connect("dev-b", false);
        gw.subscribe("dev-b", "sport/#", Qos.AT_LEAST_ONCE);

        PublishResult r1 = gw.publish(PublishRequest.of("sport/tennis/player1", Qos.AT_LEAST_ONCE, "m1"));
        GatewayTestSupport.logPublish("路由1",
                PublishRequest.of("sport/tennis/player1", Qos.AT_LEAST_ONCE, "m1"), r1);
        assertEquals(2, r1.outcomes().size());

        PublishResult r2 = gw.publish(PublishRequest.of("sport", Qos.AT_LEAST_ONCE, "m2"));
        GatewayTestSupport.logPublish("路由2",
                new PublishRequest("sport", Qos.AT_LEAST_ONCE, "m2".getBytes()), r2);
        // 'sport/tennis/+' 不匹配 'sport'；'sport/#' 匹配
        assertEquals(1, r2.outcomes().size());
        assertEquals("dev-b", r2.outcomes().get(0).clientId());
    }
}
