package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 容量与背压卡死、互不牵连：
 * 任何被接受的发布都不允许实际积压量/在途量超上限；超限在产生任何投递前整体拒绝；
 * 单个设备占满自己的额度不影响其他设备的发布、订阅与确认。
 */
class CapacityIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityIsolationTest.class);

    @Test
    void backlogNeverExceedsPerSessionCapacityEvenWithMultipleOfflineTargets() {
        // 三个离线持久订阅者，容量均为 2；一条消息同时命中三者。
        // 其中一个已满 -> 整批拒绝，其他两个一条都不能收到（不投一半留一半）
        GatewayConfig config = new GatewayConfig();
        config.setBacklogCapacity(2);
        Gateway gateway = new Gateway(config);
        for (String c : List.of("d1", "d2", "d3")) {
            gateway.connect(c, true);
            gateway.subscribe(c, "t/#", QoS.AT_MOST_ONCE);
            gateway.disconnect(c);
        }
        gateway.publish("t/a", "m1"); // 三者积压各 1
        gateway.publish("t/b", "m2"); // 三者积压各 2（满）
        assertEquals(2, gateway.backlogSize("d1"));
        assertEquals(2, gateway.backlogSize("d2"));
        assertEquals(2, gateway.backlogSize("d3"));

        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.publish("t/c", "m3"));
        log.info("[cap-strict] 三目标积压均 2/2 时再发布 -> reason={} 各会话积压={}/{}/{}",
                ex.reason(), gateway.backlogSize("d1"), gateway.backlogSize("d2"),
                gateway.backlogSize("d3"));
        assertEquals(RejectReason.BACKLOG_CAPACITY_EXCEEDED, ex.reason());
        // 没有任何一个会话多出一条
        assertEquals(2, gateway.backlogSize("d1"));
        assertEquals(2, gateway.backlogSize("d2"));
        assertEquals(2, gateway.backlogSize("d3"));
    }

    @Test
    void oneFullDeviceDoesNotBlockOtherDevices() {
        GatewayConfig config = new GatewayConfig();
        config.setBacklogCapacity(1);
        config.setMaxInFlight(2);
        Gateway gateway = new Gateway(config);

        // slow：离线持久，订阅 busy/#，积压 1/1 打满
        gateway.connect("slow", true);
        gateway.subscribe("slow", "busy/#", QoS.AT_MOST_ONCE);
        gateway.disconnect("slow");
        gateway.publish("busy/x", "m1");
        assertEquals(1, gateway.backlogSize("slow"));

        // fast：在线，订阅 other/#，可正常发布与确认（额度互不牵连）
        gateway.connect("fast", false);
        gateway.subscribe("fast", "other/#", QoS.AT_LEAST_ONCE);
        List<String> routed = gateway.publish("other/x", "ok");
        assertEquals(List.of("fast"), routed);
        gateway.ack("fast", routed.isEmpty() ? -1L : 2L); // busy/x=id1, other/x=id2
        log.info("[isolation] slow 积压满后 fast 发布={} 并确认成功 inFlight={}",
                routed, gateway.inFlightCount());
        assertEquals(0, gateway.pendingAckCount("fast"));

        // 新设备仍可连接与订阅，且其发布不受 slow 影响
        gateway.connect("late", false);
        gateway.subscribe("late", "other/#", QoS.AT_MOST_ONCE);
        routed = gateway.publish("other/y", "late-ok");
        assertEquals(List.of("fast", "late"), routed);
        log.info("[isolation] 后到设备 late 正常订阅发布 routed={}", routed);

        // slow 主题的发布仍被明确拒绝，且 slow 状态不变
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.publish("busy/y", "m2"));
        assertEquals(RejectReason.BACKLOG_CAPACITY_EXCEEDED, ex.reason());
        assertEquals(1, gateway.backlogSize("slow"));
        log.info("[isolation] slow 仍被明确拒绝 reason={}，其余设备全程可用", ex.reason());
    }

    @Test
    void inFlightNeverExceedsGlobalLimitAcceptedPublishesOnly() {
        // maxInFlight=3：c1 至少一次、c2 至多一次；c2 永不占在途
        GatewayConfig config = new GatewayConfig();
        config.setMaxInFlight(3);
        Gateway gateway = new Gateway(config);
        gateway.connect("c1", false);
        gateway.connect("c2", false);
        gateway.subscribe("c1", "t/#", QoS.AT_LEAST_ONCE);
        gateway.subscribe("c2", "t/#", QoS.AT_MOST_ONCE);

        for (int i = 0; i < 3; i++) {
            gateway.publish("t/" + i, "m" + i, QoS.AT_LEAST_ONCE);
        }
        assertEquals(3, gateway.inFlightCount(), "被接受的发布不得使在途超过上限");
        // 再发将新增 1（仅 c1）：3+1=4 > 3，拒绝
        GatewayException ex = assertThrows(GatewayException.class,
                () -> gateway.publish("t/3", "m3", QoS.AT_LEAST_ONCE));
        assertEquals(RejectReason.BACKPRESSURE_LIMIT_EXCEEDED, ex.reason());
        log.info("[inflight-strict] inFlight=3/3 再发布(新增1) -> reason={} inFlight 仍={}",
                ex.reason(), gateway.inFlightCount());
        assertEquals(3, gateway.inFlightCount());
        assertEquals(3, gateway.pendingAckCount("c1"));

        // 发布方至多一次时 c1 也协商为至多一次：不占在途，发布应被接受
        List<String> routed = gateway.publish("t/q0", "m-q0", QoS.AT_MOST_ONCE);
        assertEquals(List.of("c1", "c2"), routed);
        assertEquals(3, gateway.inFlightCount(), "至多一次协商结果不占在途");
        log.info("[inflight-strict] 发布方至多一次时不占额度，发布接受 inFlight={}",
                gateway.inFlightCount());
    }
}
