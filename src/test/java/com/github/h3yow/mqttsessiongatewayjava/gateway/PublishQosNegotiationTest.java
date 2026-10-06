package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 发布侧 QoS 与两端协商：每条投递按 min(发布方 QoS, 订阅方 QoS) 处理；
 * 重连补投与确认也按协商后的等级走。
 */
class PublishQosNegotiationTest {

    private static final Logger log = LoggerFactory.getLogger(PublishQosNegotiationTest.class);

    @Test
    void negotiatedQosIsTheLowerOfPublisherAndSubscriber() {
        // 四个订阅者：两种订阅 QoS，覆盖四种组合
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("sub0", false);
        gateway.connect("sub1", false);
        gateway.connect("sub0b", false);
        gateway.connect("sub1b", false);
        gateway.subscribe("sub0", "t/#", QoS.AT_MOST_ONCE);
        gateway.subscribe("sub1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.subscribe("sub0b", "x/#", QoS.AT_MOST_ONCE);
        gateway.subscribe("sub1b", "x/#", QoS.AT_LEAST_ONCE);

        gateway.publish("t/a", "pub-at-least", QoS.AT_LEAST_ONCE);
        log.info("[negotiate] pub=AT_LEAST_ONCE: sub0(AT_MOST) pending={} sub1(AT_LEAST) pending={}",
                gateway.pendingAckCount("sub0"), gateway.pendingAckCount("sub1"));
        // pub1 + sub0 -> 至多一次：不跟踪；pub1 + sub1 -> 至少一次：待确认
        assertEquals(0, gateway.pendingAckCount("sub0"));
        assertEquals(1, gateway.pendingAckCount("sub1"));
        assertEquals(1, gateway.inFlightCount());

        gateway.publish("x/a", "pub-at-most", QoS.AT_MOST_ONCE);
        log.info("[negotiate] pub=AT_MOST_ONCE: sub0b(AT_MOST) pending={} sub1b(AT_LEAST) pending={}",
                gateway.pendingAckCount("sub0b"), gateway.pendingAckCount("sub1b"));
        // pub0 + sub1 -> 至多一次：即使订阅要求至少一次也不跟踪
        assertEquals(0, gateway.pendingAckCount("sub0b"));
        assertEquals(0, gateway.pendingAckCount("sub1b"));
        assertEquals(1, gateway.inFlightCount());
    }

    @Test
    void negotiatedAtMostOnceOfflineBacklogIsRedeliveredWithoutAck() {
        // 发布方至多一次 + 订阅方至少一次 => 协商为至多一次：
        // 离线积压补投一次，重连后不进入待确认
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("dev1", true);
        gateway.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.disconnect("dev1");

        gateway.publish("t/a", "m1", QoS.AT_MOST_ONCE);
        assertEquals(1, gateway.backlogSize("dev1"));

        List<Message> redelivered = gateway.connect("dev1", true);
        log.info("[nego-offline] pub0+sub1 重连补投 ids={} pending={} (expect 补投1条、无需确认)",
                redelivered.stream().map(Message::id).toList(),
                gateway.pendingAckCount("dev1"));
        assertEquals(1, redelivered.size());
        assertEquals(0, gateway.pendingAckCount("dev1"));
        assertEquals(0, gateway.inFlightCount());
        // 再次重连没有补投：补投恰好一次
        gateway.disconnect("dev1");
        assertTrue(gateway.connect("dev1", true).isEmpty());
    }

    @Test
    void negotiatedAtLeastOnceUnackedIsRedeliveredAfterReconnectAndAckWorks() {
        // 发布方至少一次 + 订阅方至少一次 => 至少一次：掉线重投、确认后消失
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("dev1", true);
        gateway.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.publish("t/a", "m1", QoS.AT_LEAST_ONCE);
        gateway.disconnect("dev1");

        List<Message> redelivered = gateway.connect("dev1", true);
        assertEquals(List.of(1L), redelivered.stream().map(Message::id).toList());
        assertEquals(1, gateway.pendingAckCount("dev1"));
        gateway.ack("dev1", 1L);
        log.info("[nego-unacked] pub1+sub1 重投后确认成功 pending={} inFlight={}",
                gateway.pendingAckCount("dev1"), gateway.inFlightCount());
        assertEquals(0, gateway.pendingAckCount("dev1"));
        assertEquals(0, gateway.inFlightCount());
        gateway.disconnect("dev1");
        assertTrue(gateway.connect("dev1", true).isEmpty());
    }

    @Test
    void defaultPublishTreatsPublisherAsAtLeastOnce() {
        // 不带 QoS 的旧签名 publish(topic, payload)：发布方按至少一次，
        // 因此订阅至少一次时仍有待确认行为（向后兼容）
        Gateway gateway = new Gateway(new GatewayConfig());
        gateway.connect("c1", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.publish("t/a", "m1");
        log.info("[default-pub] 旧签名发布 pending={} inFlight={} (expect 1/1)",
                gateway.pendingAckCount("c1"), gateway.inFlightCount());
        assertEquals(1, gateway.pendingAckCount("c1"));
        assertEquals(1, gateway.inFlightCount());
    }
}
