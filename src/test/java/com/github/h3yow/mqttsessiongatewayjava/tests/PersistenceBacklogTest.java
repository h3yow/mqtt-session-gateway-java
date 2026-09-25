package com.github.h3yow.mqttsessiongatewayjava.tests;

import com.github.h3yow.mqttsessiongatewayjava.gateway.MqttGateway;
import com.github.h3yow.mqttsessiongatewayjava.gateway.config.GatewayProperties;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.DeliveryDecision;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.OperationResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishRequest;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 持久会话与离线积压：
 * 离线消息进入积压且不丢；重连后以 redelivery 投递；ACK 不重复；
 * 网关“重启”后状态可恢复；cleanStart 清除旧状态。
 */
class PersistenceBacklogTest {

    private static final Logger log = LoggerFactory.getLogger(PersistenceBacklogTest.class);

    @Test
    void offlineMessagesAreBackloggedAndRedeliveredOnce(@org.junit.jupiter.api.io.TempDir Path dir) {
        GatewayProperties props = GatewayTestSupport.properties(dir, 1000);
        RecordingDeliveryHandler handler = new RecordingDeliveryHandler();
        MqttGateway gw = GatewayTestSupport.gateway(props, handler);

        gw.connect("dev-1", false);
        gw.subscribe("dev-1", "alerts/#", Qos.AT_LEAST_ONCE);
        gw.disconnect("dev-1");

        // 离线期间发布：QoS1 进入积压；QoS0 按至多一次明确跳过（不积压、非静默）
        PublishRequest m1 = PublishRequest.of("alerts/fire", Qos.AT_LEAST_ONCE, "fire-1");
        PublishRequest m2 = PublishRequest.of("alerts/smoke", Qos.AT_MOST_ONCE, "smoke-1");
        PublishResult r1 = gw.publish(m1);
        PublishResult r2 = gw.publish(m2);
        GatewayTestSupport.logPublish("离线发布1", m1, r1);
        GatewayTestSupport.logPublish("离线发布2", m2, r2);
        assertEquals(DeliveryDecision.STORED_OFFLINE, r1.outcomes().get(0).decision());
        assertEquals(DeliveryDecision.SKIPPED, r2.outcomes().get(0).decision());
        GatewayTestSupport.logView("离线积压状态", gw, "dev-1");
        assertEquals(1, gw.sessionView("dev-1").pipelineSize());

        // 非持久离线客户端不保留：connect(cleanStart=true) → subscribe → disconnect
        gw.connect("dev-tmp", true);
        gw.subscribe("dev-tmp", "temp-only/+", Qos.AT_LEAST_ONCE);
        gw.disconnect("dev-tmp");
        PublishResult r3 = gw.publish(PublishRequest.of("temp-only/1", Qos.AT_LEAST_ONCE, "t"));
        // 非持久会话断连后已从注册表移除，无任何匹配订阅者（非静默：结果列表为空）
        log.info("非持久会话离线后路由结果: {}", r3.outcomes());
        assertTrue(r3.outcomes().isEmpty());
        assertEquals(1, gw.sessionView("dev-1").pipelineSize());

        // 重连：QoS1 积压被投递（带 redelivery）；QoS0 离线消息不补投
        gw.connect("dev-1", false);
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(3),
                () -> handler.of("dev-1").size() == 1),
                "重连后应收到 1 条 QoS1 积压消息");
        List<RecordingDeliveryHandler.Delivery> got = handler.of("dev-1");
        log.info("重连后收到: {}", got);
        assertEquals(List.of("alerts/fire"),
                got.stream().map(RecordingDeliveryHandler.Delivery::topic).toList());
        assertTrue(got.stream().allMatch(RecordingDeliveryHandler.Delivery::redelivery),
                "离线积压重投必须带 redelivery 标志");

        // QoS1 那条确认；重复确认被拒（先等待投递结果登记 inflight）
        long qos1Packet = got.get(0).packetId();
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(2),
                () -> gw.sessionView("dev-1").inflightSize() == 1));
        OperationResult ack = gw.acknowledge("dev-1", qos1Packet);
        assertTrue(ack.accepted());
        OperationResult dup = gw.acknowledge("dev-1", qos1Packet);
        assertEquals(RejectReason.DUPLICATE_ACK, dup.reason());

        gw.shutdown();
    }

    @Test
    void unackedInflightIsRedeliveredAfterReconnectAndNotDuplicated(@org.junit.jupiter.api.io.TempDir Path dir) {
        GatewayProperties props = GatewayTestSupport.properties(dir, 1000);
        RecordingDeliveryHandler handler = new RecordingDeliveryHandler();
        MqttGateway gw = GatewayTestSupport.gateway(props, handler);

        gw.connect("dev-2", false);
        gw.subscribe("dev-2", "cmd/+", Qos.AT_LEAST_ONCE);
        gw.publish(PublishRequest.of("cmd/reboot", Qos.AT_LEAST_ONCE, "do"));
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(3),
                () -> gw.sessionView("dev-2").inflightSize() == 1));
        long pid = gw.sessionView("dev-2").nextPacketId() - 1;
        GatewayTestSupport.logView("收到但未确认即断连", gw, "dev-2");

        // 未 ACK 断连
        gw.disconnect("dev-2");
        assertEquals(1, gw.sessionView("dev-2").pipelineSize());

        // 重连：应再次收到同 packetId 且 redelivery=true，只投递一次
        gw.connect("dev-2", false);
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(3),
                () -> handler.of("dev-2").size() == 2));
        var deliveries = handler.of("dev-2");
        assertEquals(2, deliveries.size());
        assertEquals(pid, deliveries.get(1).packetId());
        assertTrue(deliveries.get(1).redelivery());
        log.info("同一 packetId 重投: 首次={} 重投={}", deliveries.get(0), deliveries.get(1));

        // 确认后不应再有重投
        OperationResult ack = gw.acknowledge("dev-2", pid);
        assertTrue(ack.accepted());
        gw.disconnect("dev-2");
        gw.connect("dev-2", false);
        GatewayTestSupport.await(Duration.ofSeconds(1), () -> true);
        assertEquals(2, handler.of("dev-2").size(), "已确认消息不得再次投递");
        gw.shutdown();
    }

    @Test
    void stateSurvivesGatewayRestart(@org.junit.jupiter.api.io.TempDir Path dir) {
        GatewayProperties props = GatewayTestSupport.properties(dir, 1000);
        RecordingDeliveryHandler h1 = new RecordingDeliveryHandler();
        MqttGateway gw1 = GatewayTestSupport.gateway(props, h1);
        gw1.connect("dev-3", false);
        gw1.subscribe("dev-3", "data/#", Qos.AT_LEAST_ONCE);
        gw1.disconnect("dev-3");
        PublishRequest req = PublishRequest.of("data/temp", Qos.AT_LEAST_ONCE, "21.5");
        PublishResult r = gw1.publish(req);
        GatewayTestSupport.logPublish("重启前离线发布", req, r);
        long packetId = r.outcomes().get(0).packetId();
        gw1.shutdown();

        // 模拟重启：同一 dataDir 构建新网关
        RecordingDeliveryHandler h2 = new RecordingDeliveryHandler();
        MqttGateway gw2 = GatewayTestSupport.gateway(props, h2);
        GatewayTestSupport.logView("重启恢复后(未连接)", gw2, "dev-3");
        assertEquals(1, gw2.sessionView("dev-3").pipelineSize());
        assertTrue(gw2.sessionView("dev-3").subscriptions().containsKey("data/#"),
                "订阅必须恢复");

        gw2.connect("dev-3", false);
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(3),
                () -> h2.of("dev-3").size() == 1));
        var d = h2.of("dev-3").get(0);
        assertEquals(packetId, d.packetId(), "packetId 必须跨重启稳定");
        assertEquals("data/temp", d.topic());
        assertTrue(d.redelivery());
        log.info("重启后投递判定: {}", d);

        OperationResult ack = gw2.acknowledge("dev-3", packetId);
        assertTrue(ack.accepted());
        gw2.shutdown();

        // 再重启：已确认消息不得复活
        RecordingDeliveryHandler h3 = new RecordingDeliveryHandler();
        MqttGateway gw3 = GatewayTestSupport.gateway(props, h3);
        gw3.connect("dev-3", false);
        GatewayTestSupport.await(Duration.ofSeconds(1), () -> true);
        assertTrue(h3.of("dev-3").isEmpty(), "ACK 已落盘，重启后不得重复投递");
        GatewayTestSupport.logView("二次重启", gw3, "dev-3");
        gw3.shutdown();
    }

    @Test
    void cleanStartWipesPersistentState(@org.junit.jupiter.api.io.TempDir Path dir) {
        GatewayProperties props = GatewayTestSupport.properties(dir, 1000);
        MqttGateway gw = GatewayTestSupport.gateway(props, new RecordingDeliveryHandler());
        gw.connect("dev-4", false);
        gw.subscribe("dev-4", "old/#", Qos.AT_LEAST_ONCE);
        gw.disconnect("dev-4");
        gw.publish(PublishRequest.of("old/x", Qos.AT_LEAST_ONCE, "m"));
        assertEquals(1, gw.sessionView("dev-4").pipelineSize());

        OperationResult connected = gw.connect("dev-4", true);
        assertTrue(connected.accepted());
        assertTrue(gw.sessionView("dev-4").subscriptions().isEmpty());
        assertEquals(0, gw.sessionView("dev-4").pipelineSize());
        assertFalse(gw.sessionView("dev-4").persistent());
        GatewayTestSupport.logView("cleanStart 后", gw, "dev-4");
        gw.shutdown();
    }
}
