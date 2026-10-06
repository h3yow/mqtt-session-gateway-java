package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真正的重启恢复：关闭一个 Gateway 实例后，在同一数据目录上新建第二个实例，
 * 验证持久会话的订阅、离线积压、未确认消息、已确认记录原样恢复。
 * 全程只使用本地临时目录，不依赖任何外部服务。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    private GatewayConfig configFor(Path dataDir) {
        GatewayConfig config = new GatewayConfig();
        config.setDataDir(dataDir.toString());
        return config;
    }

    @Test
    void persistentSubscriptionsBacklogAndPendingAckSurviveRealRestart(@TempDir Path dataDir) {
        // ---- 进程 1：建立持久会话，制造离线积压与未确认消息 ----
        Gateway first = new Gateway(configFor(dataDir));
        first.connect("dev1", true);
        first.subscribe("dev1", "telemetry/#", QoS.AT_LEAST_ONCE);
        first.subscribe("dev1", "cmd/+", QoS.AT_MOST_ONCE);
        // 在线收到 id=1，未确认
        first.publish("telemetry/online", "online-msg");
        assertEquals(1, first.pendingAckCount("dev1"));
        // 离线后两条积压：telemetry 走至少一次，cmd 走至多一次
        first.disconnect("dev1");
        first.publish("telemetry/a", "offline-1");
        first.publish("cmd/reboot", "offline-2");
        assertEquals(2, first.backlogSize("dev1"));
        log.info("[restart-1] 进程1停机前 dev1 backlog=2 pendingAck=1 数据目录={}", dataDir);

        // ---- 进程 2：全新实例，指向同一数据目录 ----
        Gateway second = new Gateway(configFor(dataDir));
        log.info("[restart-2] 进程2已启动（新对象、同数据目录）backlog={} pending={}",
                second.backlogSize("dev1"), second.pendingAckCount("dev1"));
        // 恢复后离线：恢复期间到达的发布不应路由（会话离线 -> 入积压）
        // 积压容量恢复：
        assertEquals(2, second.backlogSize("dev1"));
        assertEquals(1, second.pendingAckCount("dev1")); // id=1 未确认
        // 在途额度同样恢复：未确认消息仍占在途
        assertEquals(1, second.inFlightCount());

        List<Message> redelivered = second.connect("dev1", true);
        List<Long> ids = redelivered.stream().map(Message::id).sorted().toList();
        log.info("[restart-2] dev1 重连补投 ids={} (expect [1,2,3]：不丢不重)", ids);
        assertEquals(List.of(1L, 2L, 3L), ids);
        assertEquals(0, second.backlogSize("dev1"));
        // id=1（telemetry，至少一次）与 id=2（telemetry，至少一次）需确认；
        // id=3（cmd，至多一次）补投后不跟踪
        assertEquals(2, second.pendingAckCount("dev1"));

        // 订阅也恢复了：重连后新消息照常路由，且 id 继续递增不复用
        List<String> routed = second.publish("telemetry/b", "after-restart");
        assertEquals(List.of("dev1"), routed);
        assertEquals(3, second.pendingAckCount("dev1"));
        log.info("[restart-2] 订阅恢复，新消息 id=4 已投递并待确认 pending={}",
                second.pendingAckCount("dev1"));
    }

    @Test
    void acknowledgedMessagesDoNotResurfaceAfterRestart(@TempDir Path dataDir) {
        Gateway first = new Gateway(configFor(dataDir));
        first.connect("dev1", true);
        first.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
        first.publish("t/a", "m1"); // id=1
        first.publish("t/b", "m2"); // id=2
        first.ack("dev1", 1L);      // 仅确认 id=1
        first.disconnect("dev1");
        log.info("[restart-ack-1] 停机前：id=1 已确认, id=2 未确认");

        Gateway second = new Gateway(configFor(dataDir));
        List<Message> redelivered = second.connect("dev1", true);
        List<Long> ids = redelivered.stream().map(Message::id).toList();
        log.info("[restart-ack-2] 重连补投 ids={} (expect [2]：已确认的 id=1 不再冒出来)", ids);
        assertEquals(List.of(2L), ids);
        second.ack("dev1", 2L);

        // 对重启前已确认的 id=1 再确认：必须识别为重复确认
        GatewayException ex = assertThrows(GatewayException.class, () -> second.ack("dev1", 1L));
        log.info("[restart-ack-2] 重启后对历史已确认 id=1 再确认 -> reason={}", ex.reason());
        assertEquals(RejectReason.ACK_DUPLICATE, ex.reason());
    }

    @Test
    void nonPersistentSessionIsWipedAcrossRestart(@TempDir Path dataDir) {
        Gateway first = new Gateway(configFor(dataDir));
        first.connect("persist", true);
        first.subscribe("persist", "t/#", QoS.AT_LEAST_ONCE);
        first.connect("ephemeral", false);
        first.subscribe("ephemeral", "t/#", QoS.AT_LEAST_ONCE);
        first.disconnect("persist");
        first.disconnect("ephemeral"); // 非持久：断开即清
        first.publish("t/a", "m1");
        log.info("[restart-np-1] 停机前 persist.backlog={} ephemeral 已删除",
                first.backlogSize("persist"));

        Gateway second = new Gateway(configFor(dataDir));
        // 持久会话恢复、离线、持有积压
        assertEquals(1, second.backlogSize("persist"));
        // 非持久会话不存在：操作被拒绝
        GatewayException ex = assertThrows(GatewayException.class,
                () -> second.subscribe("ephemeral", "t/#", QoS.AT_LEAST_ONCE));
        log.info("[restart-np-2] 非持久会话重启后操作 -> reason={}", ex.reason());
        assertEquals(RejectReason.SESSION_NOT_FOUND, ex.reason());
        // 落盘文件中也不应包含非持久会话的任何痕迹
        assertTrue(second.backlogSize("ephemeral") == 0);
    }

    @Test
    void restartRepeatedlyKeepsStateStableAndDurable(@TempDir Path dataDir) throws IOException {
        Gateway g = new Gateway(configFor(dataDir));
        g.connect("dev1", true);
        g.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
        g.disconnect("dev1");
        g.publish("t/a", "m1");
        g.publish("t/b", "m2");

        // 连续多次“重启”，每次都是全新实例
        for (int i = 0; i < 3; i++) {
            g = new Gateway(configFor(dataDir));
            assertEquals(2, g.backlogSize("dev1"), "第 " + i + " 次重启后积压仍为 2");
            log.info("[restart-loop] 第{}次重启 backlog=2 状态稳定", i + 1);
        }
        // 快照文件确实落在了本地目录
        try (var files = Files.list(dataDir)) {
            boolean hasSnapshot = files.anyMatch(p -> p.getFileName().toString()
                    .startsWith("gateway-state.json"));
            assertTrue(hasSnapshot, "本地快照文件应存在于 " + dataDir);
        }
    }
}
