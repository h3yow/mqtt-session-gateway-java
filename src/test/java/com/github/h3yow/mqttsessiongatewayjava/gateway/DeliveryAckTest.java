package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 投递与确认语义：路由不投递给不匹配订阅；至多一次/至少一次区分；
 * 重复确认与非对应订阅者的确认以可区分原因拒绝。
 */
class DeliveryAckTest {

    private static final Logger log = LoggerFactory.getLogger(DeliveryAckTest.class);

    private Gateway gateway;

    @BeforeEach
    void setUp() {
        gateway = new Gateway(new GatewayConfig());
    }

    @Test
    void routesOnlyToMatchingSubscriptions() {
        gateway.connect("c1", false);
        gateway.connect("c2", false);
        gateway.connect("c3", false);
        gateway.subscribe("c1", "sensors/+/temp", QoS.AT_MOST_ONCE);
        gateway.subscribe("c2", "sensors/#", QoS.AT_MOST_ONCE);
        gateway.subscribe("c3", "actuators/#", QoS.AT_MOST_ONCE);

        List<String> routed = gateway.publish("sensors/room1/temp", "21.5");
        log.info("[route] topic='sensors/room1/temp' subs={c1:'sensors/+/temp', c2:'sensors/#', c3:'actuators/#'}"
                + " -> routed={}", routed);
        assertEquals(List.of("c1", "c2"), routed);

        routed = gateway.publish("actuators/door", "open");
        log.info("[route] topic='actuators/door' -> routed={}", routed);
        assertEquals(List.of("c3"), routed);
    }

    @Test
    void atMostOnceNeedsNoAckAndIsNotTracked() {
        gateway.connect("c1", false);
        gateway.subscribe("c1", "t/#", QoS.AT_MOST_ONCE);
        gateway.publish("t/a", "m1");
        log.info("[qos0] pendingAck={} inFlight={} (expect 0/0: 至多一次不跟踪)",
                gateway.pendingAckCount("c1"), gateway.inFlightCount());
        assertEquals(0, gateway.pendingAckCount("c1"));
        assertEquals(0, gateway.inFlightCount());
    }

    @Test
    void atLeastOnceIsTrackedUntilAcked() {
        gateway.connect("c1", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.publish("t/a", "m1");
        log.info("[qos1] after publish pendingAck={} inFlight={} (expect 1/1)",
                gateway.pendingAckCount("c1"), gateway.inFlightCount());
        assertEquals(1, gateway.pendingAckCount("c1"));
        assertEquals(1, gateway.inFlightCount());

        gateway.ack("c1", 1L);
        log.info("[qos1] after ack(1) pendingAck={} inFlight={} (expect 0/0)",
                gateway.pendingAckCount("c1"), gateway.inFlightCount());
        assertEquals(0, gateway.pendingAckCount("c1"));
        assertEquals(0, gateway.inFlightCount());
    }

    @Test
    void duplicateAckRejectedWithDistinguishableReason() {
        gateway.connect("c1", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.publish("t/a", "m1");
        gateway.ack("c1", 1L);

        GatewayException ex = assertThrows(GatewayException.class, () -> gateway.ack("c1", 1L));
        log.info("[ack-dup] client='c1' messageId=1 -> reason={} detail='{}'", ex.reason(), ex.getMessage());
        assertEquals(RejectReason.ACK_DUPLICATE, ex.reason());
    }

    @Test
    void ackFromWrongSubscriberRejectedWithDistinguishableReason() {
        gateway.connect("c1", false);
        gateway.connect("c2", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.subscribe("c2", "other/#", QoS.AT_LEAST_ONCE);
        gateway.publish("t/a", "m1"); // 只路由给 c1，messageId=1

        GatewayException ex = assertThrows(GatewayException.class, () -> gateway.ack("c2", 1L));
        log.info("[ack-wrong] client='c2' messageId=1 (pending for 'c1') -> reason={} detail='{}'",
                ex.reason(), ex.getMessage());
        assertEquals(RejectReason.ACK_NOT_PENDING_FOR_CLIENT, ex.reason());
        // c1 的待确认不受 c2 的错误确认影响
        assertEquals(1, gateway.pendingAckCount("c1"));
    }

    @Test
    void ackUnknownMessageRejected() {
        gateway.connect("c1", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        GatewayException ex = assertThrows(GatewayException.class, () -> gateway.ack("c1", 999L));
        log.info("[ack-unknown] client='c1' messageId=999 -> reason={}", ex.reason());
        assertEquals(RejectReason.ACK_NOT_PENDING_FOR_CLIENT, ex.reason());
    }

    @Test
    void operationsOnUnknownSessionRejected() {
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.subscribe("ghost", "t/#", QoS.AT_MOST_ONCE));
        log.info("[no-session] subscribe client='ghost' -> reason={}", ex.reason());
        assertEquals(RejectReason.SESSION_NOT_FOUND, ex.reason());

        ex = assertThrows(GatewayException.class, () -> gateway.ack("ghost", 1L));
        log.info("[no-session] ack client='ghost' -> reason={}", ex.reason());
        assertEquals(RejectReason.SESSION_NOT_FOUND, ex.reason());
    }

    @Test
    void invalidFilterRejectedOnSubscribe() {
        gateway.connect("c1", false);
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.subscribe("c1", "a/#/b", QoS.AT_MOST_ONCE));
        log.info("[bad-filter] filter='a/#/b' -> reason={}", ex.reason());
        assertEquals(RejectReason.FILTER_WILDCARD_MISPLACED, ex.reason());
    }

    @Test
    void nonPersistentSessionLosesStateOnDisconnect() {
        gateway.connect("c1", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.disconnect("c1");
        List<String> routed = gateway.publish("t/a", "m1");
        log.info("[non-persistent] routed={} (expect empty: 会话已清除)", routed);
        assertTrue(routed.isEmpty());
    }
}
