package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 发布侧 QoS 与双边协商：每条投递按 min(发布QoS, 订阅QoS) 执行；
 * 重连补投与确认均按协商后的等级走；至多一次投递不占用在途额度。
 */
class PublishQosNegotiationTest {

    private static final Logger log = LoggerFactory.getLogger(PublishQosNegotiationTest.class);

    @Test
    void effectiveQosIsTheLowerOfPublisherAndSubscriber() {
        Gateway gw = new Gateway(new GatewayConfig());
        gw.connect("qos1sub", false);
        gw.connect("qos0sub", false);
        gw.subscribe("qos1sub", "t/#", QoS.AT_LEAST_ONCE);
        gw.subscribe("qos0sub", "t/#", QoS.AT_MOST_ONCE);

        // 发布方 QoS1：QoS1 订阅者协商为 1（进在途），QoS0 订阅者协商为 0（不跟踪）
        List<String> routed = gw.publish("t/a", "m", QoS.AT_LEAST_ONCE);
        log.info("[negotiate] pub=1 subs={qos1sub:1,qos0sub:0} -> routed={} pending(qos1sub)={} pending(qos0sub)={} inFlight={}",
                routed, gw.pendingAckCount("qos1sub"), gw.pendingAckCount("qos0sub"), gw.inFlightCount());
        assertEquals(List.of("qos1sub", "qos0sub"), routed);
        assertEquals(1, gw.pendingAckCount("qos1sub"));
        assertEquals(0, gw.pendingAckCount("qos0sub"));
        assertEquals(1, gw.inFlightCount());

        // 发布方 QoS0：两边都协商为 0，全部不跟踪
        gw.publish("t/b", "m", QoS.AT_MOST_ONCE);
        log.info("[negotiate] pub=0 -> pending(qos1sub)={} inFlight={} (expect 1/1: 本次无人进在途)",
                gw.pendingAckCount("qos1sub"), gw.inFlightCount());
        assertEquals(1, gw.pendingAckCount("qos1sub"));
        assertEquals(1, gw.inFlightCount());
    }

    @Test
    void twoArgPublishDefaultsToAtLeastOnce() {
        Gateway gw = new Gateway(new GatewayConfig());
        gw.connect("c", false);
        gw.subscribe("c", "t/#", QoS.AT_LEAST_ONCE);
        gw.publish("t/a", "m"); // 不带 QoS 的重载：默认至少一次
        log.info("[default-qos] publish(topic,payload) -> pending={} inFlight={} (expect 1/1)",
                gw.pendingAckCount("c"), gw.inFlightCount());
        assertEquals(1, gw.pendingAckCount("c"));
        assertEquals(1, gw.inFlightCount());
    }

    @Test
    void negotiatedQos0OfflineIsBackloggedButNeedsNoAckOnRedelivery() {
        Gateway gw = new Gateway(new GatewayConfig());
        gw.connect("d", true);
        gw.subscribe("d", "t/#", QoS.AT_LEAST_ONCE); // 订阅方要 1
        gw.disconnect("d");
        gw.publish("t/a", "m0", QoS.AT_MOST_ONCE); // 发布方只给 0 -> 协商 0
        gw.publish("t/b", "m1", QoS.AT_LEAST_ONCE); // 协商 1

        List<Message> redelivered = gw.connect("d", true);
        log.info("[offline-nego] 补投 ids={} pending={} (expect [1,2] 与 1: 仅协商为1的进待确认)",
                redelivered.stream().map(Message::id).toList(), gw.pendingAckCount("d"));
        assertEquals(2, redelivered.size());
        assertEquals(1, gw.pendingAckCount("d"));
        // 对协商为 0 的 id=1 做确认：它本就不是待确认，必须拒绝
        GatewayException ex = assertThrows(GatewayException.class, () -> gw.ack("d", 1L));
        assertEquals(RejectReason.ACK_NOT_PENDING_FOR_CLIENT, ex.reason());
        gw.ack("d", 2L);
        assertEquals(0, gw.pendingAckCount("d"));
    }

    @Test
    void qos0DeliveryDoesNotConsumeInFlightAndIsAllowedAtLimit() {
        GatewayConfig config = new GatewayConfig();
        config.setMaxInFlight(1);
        Gateway gw = new Gateway(config);
        gw.connect("a", false);
        gw.connect("b", false);
        gw.subscribe("a", "t/#", QoS.AT_LEAST_ONCE);
        gw.subscribe("b", "t/#", QoS.AT_MOST_ONCE);

        gw.publish("t/a", "m1", QoS.AT_LEAST_ONCE); // a 占 1 个在途，b 协商 0 不占 -> 在途 1/1
        assertEquals(1, gw.inFlightCount());
        // 再发一条 QoS1：会让 a 的在途变成 2 > 1，拒绝
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gw.publish("t/b", "m2", QoS.AT_LEAST_ONCE));
        log.info("[qos0-bp] QoS1 发布 reason={} (a 将超限)", ex.reason());
        assertEquals(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED, ex.reason());
        // 同一条以 QoS0 发布：a 也被协商为 0，不新增在途，不超上限，应被接受
        List<String> routed = gw.publish("t/b", "m2", QoS.AT_MOST_ONCE);
        log.info("[qos0-bp] QoS0 发布 routed={} inFlight={} (expect [a,b], 1: 至多一次不受在途上限影响)",
                routed, gw.inFlightCount());
        assertEquals(List.of("a", "b"), routed);
        assertEquals(1, gw.inFlightCount());
    }

    @Test
    void qosLowerHelper() {
        assertEquals(QoS.AT_MOST_ONCE, QoS.lower(QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE));
        assertEquals(QoS.AT_MOST_ONCE, QoS.lower(QoS.AT_LEAST_ONCE, QoS.AT_MOST_ONCE));
        assertEquals(QoS.AT_LEAST_ONCE, QoS.lower(QoS.AT_LEAST_ONCE, QoS.AT_LEAST_ONCE));
    }
}
