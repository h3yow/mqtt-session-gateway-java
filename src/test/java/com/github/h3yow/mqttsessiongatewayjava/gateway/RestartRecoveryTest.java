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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真正的重启恢复：关闭一个 Gateway 实例后，new 一个全新的 Gateway 指向同一本地数据目录，
 * 验证持久会话的订阅、离线积压、未确认消息与已确认记录原样恢复。
 * 注意区别于"同进程断开再连"——这里是全新的网关对象（模拟进程重启）。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    private Gateway gatewayAt(Path dir) {
        GatewayConfig config = new GatewayConfig();
        config.setDataDir(dir.toString());
        return new Gateway(config);
    }

    @Test
    void backlogAndSubscriptionsSurviveRealRestart(@TempDir Path dir) {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("dev1", true);
            gw.subscribe("dev1", "telemetry/#", QoS.AT_LEAST_ONCE);
            gw.disconnect("dev1");
            gw.publish("telemetry/a", "m1");
            gw.publish("telemetry/b", "m2");
            assertEquals(2, gw.backlogSize("dev1"));
        }
        log.info("[restart] 第一个网关实例已关闭（模拟进程退出），数据目录={}", dir);

        // 全新实例：构造时从同一数据目录重放；恢复后会话为离线，积压与订阅原样存在
        try (Gateway gw2 = gatewayAt(dir)) {
            log.info("[restart] 新网关实例已启动，dev1 尚未连接: backlog={} subs={}",
                    gw2.backlogSize("dev1"), gw2.subscriptionCount("dev1"));
            assertEquals(2, gw2.backlogSize("dev1"), "离线积压必须跨进程恢复");
            assertEquals(1, gw2.subscriptionCount("dev1"), "订阅必须跨进程恢复");
        }

        // 再次重启后真正重连：积压两条按原 id 补投，并转入待确认
        try (Gateway gw3 = gatewayAt(dir)) {
            List<Message> first = gw3.connect("dev1", true);
            log.info("[restart] 重连补投 ids={} (expect [1,2])",
                    first.stream().map(Message::id).toList());
            assertEquals(2, first.size());
            assertEquals(List.of(1L, 2L), first.stream().map(Message::id).toList());
            assertEquals(2, gw3.pendingAckCount("dev1"));
        }
    }

    @Test
    void unackedQos1MessagesAreRedeliveredAfterRestartAndAckedOnce(@TempDir Path dir) {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("dev1", true);
            gw.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
            gw.publish("t/a", "m1"); // 在线投递，id=1，未确认
            gw.publish("t/b", "m2"); // 在线投递，id=2，确认
            gw.ack("dev1", 2L);
            assertEquals(1, gw.inFlightCount());
        }

        try (Gateway gw2 = gatewayAt(dir)) {
            List<Message> redelivered = gw2.connect("dev1", true);
            log.info("[restart-ack] 重启后补投 ids={} (expect [1]: 已确认的 2 不能又冒出来)",
                    redelivered.stream().map(Message::id).toList());
            assertEquals(List.of(1L), redelivered.stream().map(Message::id).toList());
            assertEquals(1, gw2.pendingAckCount("dev1"));
            assertEquals(1, gw2.inFlightCount()); // 重启后在途也随待确认恢复

            gw2.ack("dev1", 1L);
            assertEquals(0, gw2.inFlightCount());
        }

        // 再次重启：1 已确认、2 早已确认，均不应补投；对 1 的重复确认必须被识别
        try (Gateway gw3 = gatewayAt(dir)) {
            List<Message> redelivered = gw3.connect("dev1", true);
            log.info("[restart-ack2] 再次重启补投 count={} (expect 0: 已确认不重复)", redelivered.size());
            assertTrue(redelivered.isEmpty());
            GatewayException ex = org.junit.jupiter.api.Assertions.assertThrows(GatewayException.class,
                    () -> gw3.ack("dev1", 1L));
            log.info("[restart-ack2] 重启后对已确认消息再确认 -> reason={}", ex.reason());
            assertEquals(RejectReason.ACK_DUPLICATE, ex.reason());
        }
    }

    @Test
    void backlogQosIsNegotiatedAtPublishTimeAndSurvivesRestart(@TempDir Path dir) {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("pub", false);
            gw.connect("dev1", true);
            gw.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE); // 订阅方要求至少一次
            gw.disconnect("dev1");
            // 发布方只给至多一次：积压条目的协商 QoS 应为至多一次
            gw.publish("t/a", "m0", QoS.AT_MOST_ONCE);
            gw.publish("t/b", "m1", QoS.AT_LEAST_ONCE);
        }
        try (Gateway gw2 = gatewayAt(dir)) {
            List<Message> redelivered = gw2.connect("dev1", true);
            log.info("[restart-qos] 重启补投 ids={} (expect [1,2])",
                    redelivered.stream().map(Message::id).toList());
            assertEquals(2, redelivered.size());
            // id=1 协商为至多一次：补投即完成，不进待确认；id=2 协商为至少一次：进待确认
            assertEquals(1, gw2.pendingAckCount("dev1"));
            gw2.ack("dev1", 2L);
            assertEquals(0, gw2.pendingAckCount("dev1"));
        }
    }

    @Test
    void nonPersistentSessionsAreWipedOnRestart(@TempDir Path dir) {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("ephemeral", false);
            gw.subscribe("ephemeral", "t/#", QoS.AT_LEAST_ONCE);
            gw.disconnect("ephemeral"); // 非持久：断开即清
            gw.connect("keeper", true);
            gw.subscribe("keeper", "k/#", QoS.AT_LEAST_ONCE);
            gw.disconnect("keeper");
            gw.publish("k/a", "keep"); // keeper 离线积压
        }
        try (Gateway gw2 = gatewayAt(dir)) {
            assertEquals(0, gw2.subscriptionCount("ephemeral"), "非持久会话重启后必须不存在");
            assertEquals(0, gw2.backlogSize("ephemeral"));
            List<Message> kept = gw2.connect("keeper", true);
            log.info("[restart-np] ephemeral subs={} (expect 0), keeper redelivered={} (expect 1)",
                    gw2.subscriptionCount("ephemeral"), kept.size());
            assertEquals(1, kept.size());
        }
    }

    @Test
    void messageIdsContinueAfterRestartWithoutCollision(@TempDir Path dir) {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("c", true);
            gw.subscribe("c", "t/#", QoS.AT_MOST_ONCE);
            gw.publish("t/a", "1");
            gw.publish("t/b", "2");
        }
        try (Gateway gw2 = gatewayAt(dir)) {
            gw2.connect("c", true);
            List<String> routed = gw2.publish("t/c", "3");
            log.info("[restart-id] 重启后新消息路由={} (新 id 必须接在已用 id 之后，不与历史 id 冲突)", routed);
            // 前两条已离线积压并在重连时补投；在线新消息 id 应为 3
            assertEquals(1, routed.size());
            assertEquals(0, gw2.backlogSize("c"));
            assertEquals(0, gw2.pendingAckCount("c"));
        }
    }

    @Test
    void mixedOnlineOfflineTargetsArePersistedAtomicallyAndBackpressureRestored(@TempDir Path dir) {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("on", false);
            gw.connect("off", true);
            gw.subscribe("on", "t/#", QoS.AT_LEAST_ONCE);
            gw.subscribe("off", "t/#", QoS.AT_LEAST_ONCE);
            gw.disconnect("off"); // 在线 on + 离线 off，同一条 QoS1 发布同时打两处
            List<String> routed = gw.publish("t/a", "m1", QoS.AT_LEAST_ONCE);
            assertEquals(2, routed.size());
            assertEquals(1, gw.inFlightCount());      // on 的在途
            assertEquals(1, gw.backlogSize("off"));  // off 的积压
        }
        try (Gateway gw2 = gatewayAt(dir)) {
            // 重启后 on 是非持久会话（已消失），off 的积压恢复；积压本身不占在途
            log.info("[mixed-restart] off backlog={} inFlight={} (off 重连前)",
                    gw2.backlogSize("off"), gw2.inFlightCount());
            assertEquals(1, gw2.backlogSize("off"));
            assertEquals(0, gw2.inFlightCount());
            // off 重连：积压补投并转入在途，背压统计基于恢复后的在途继续生效
            List<Message> redelivered = gw2.connect("off", true);
            assertEquals(1, redelivered.size());
            assertEquals(1, gw2.inFlightCount());
            gw2.ack("off", redelivered.get(0).id());
            assertEquals(0, gw2.inFlightCount());
        }
    }

    @Test
    void journalIsCompactedToSnapshotOnStartup(@TempDir Path dir) throws IOException {
        try (Gateway gw = gatewayAt(dir)) {
            gw.connect("dev1", true);
            gw.subscribe("dev1", "t/#", QoS.AT_LEAST_ONCE);
            gw.disconnect("dev1");
            for (int i = 0; i < 20; i++) {
                gw.publish("t/n" + i, "m" + i);
            }
        }
        Path journal = dir.resolve("journal.jsonl");
        long sizeBefore = Files.size(journal);
        try (Gateway ignored = gatewayAt(dir)) {
            long sizeAfter = Files.size(journal);
            String firstLine = Files.readString(journal).lines().findFirst().orElse("");
            log.info("[compact] 重启前日志字节={} 重启后(快照)字节={} 首行含snapshot={}",
                    sizeBefore, sizeAfter, firstLine.contains("snapshot"));
            assertTrue(firstLine.contains("snapshot"), "重放后应压缩为单行快照");
            assertTrue(sizeAfter < sizeBefore, "20 条变更压缩后应更小");
        }
        // 快照之后还能继续正确追加
        try (Gateway gw2 = gatewayAt(dir)) {
            assertEquals(20, gw2.backlogSize("dev1"));
        }
    }
}
