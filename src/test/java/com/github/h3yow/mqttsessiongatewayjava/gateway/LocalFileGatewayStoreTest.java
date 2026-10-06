package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.GatewayStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.LocalFileGatewayStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.persist.StoredState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地文件存储：追加重放、快照压缩、崩溃残留残缺尾行容错。
 */
class LocalFileGatewayStoreTest {

    private static final Logger log = LoggerFactory.getLogger(LocalFileGatewayStoreTest.class);

    private StoredState.StoredMessage msg(long id) {
        return new StoredState.StoredMessage(id, "t/" + id, "p" + id);
    }

    @Test
    void appendAndReplayRestoresState(@TempDir Path dir) {
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            store.appendBatch(List.of(
                    StoredState.Change.persistSession("c1"),
                    StoredState.Change.addSubscription("c1",
                            new StoredState.StoredSubscription("t/#", "AT_LEAST_ONCE"))));
            store.appendBatch(List.of(StoredState.Change.enqueueBacklog("c1",
                    new StoredState.StoredBacklogEntry(msg(7), "AT_MOST_ONCE"))));
            store.appendBatch(List.of(StoredState.Change.addPending("c1", msg(8))));
            store.appendBatch(List.of(
                    StoredState.Change.removePending("c1", 8),
                    StoredState.Change.addAckedId("c1", 8)));
        }
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            StoredState.Snapshot snap = store.replay();
            StoredState.StoredSession c1 = snap.sessions().get("c1");
            log.info("[store-replay] subs={} backlog={} pending={} acked={} maxId={}",
                    c1.subscriptions().size(), c1.backlog().size(),
                    c1.pendingAck().size(), c1.ackedIds(), snap.maxMessageId());
            assertEquals(1, c1.subscriptions().size());
            assertEquals(1, c1.backlog().size());
            assertTrue(c1.pendingAck().isEmpty(), "REMOVE_PENDING 后待确认应为空");
            assertEquals(List.of(8L), c1.ackedIds(), "已确认记录必须保留");
            assertEquals(8L, snap.maxMessageId());
        }
    }

    @Test
    void tornTailLineIsIgnoredAndCompleteLinesSurvive(@TempDir Path dir) throws Exception {
        Path journal = dir.resolve("journal.jsonl");
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            store.appendBatch(List.of(StoredState.Change.persistSession("c1")));
            store.appendBatch(List.of(StoredState.Change.addPending("c1", msg(3))));
        }
        // 模拟进程在写到一半时崩溃：追加一段截断的残缺 JSON
        Files.writeString(journal,
                Files.readString(journal) + "{\"type\":\"ADD_PENDING\",\"cli", StandardCharsets.UTF_8);
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            StoredState.Snapshot snap = store.replay();
            StoredState.StoredSession c1 = snap.sessions().get("c1");
            log.info("[store-torn] 残缺尾行被跳过: pending={} maxId={}", c1.pendingAck().size(), snap.maxMessageId());
            assertEquals(1, c1.pendingAck().size(), "残缺行之前的完整状态必须保留");
            assertEquals(3L, snap.maxMessageId());

            // 压缩后日志只剩一行快照，残缺内容被清除；再重放结果一致
            store.compact(snap);
            StoredState.Snapshot again = store.replay();
            assertEquals(1, again.sessions().get("c1").pendingAck().size());
            assertEquals(3L, again.maxMessageId());
        }
    }

    @Test
    void compactionKeepsStateAndReducesJournal(@TempDir Path dir) throws Exception {
        Path journal = dir.resolve("journal.jsonl");
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            store.appendBatch(List.of(StoredState.Change.persistSession("c1")));
            for (long i = 1; i <= 10; i++) {
                store.appendBatch(List.of(StoredState.Change.addPending("c1", msg(i))));
            }
        }
        long before = Files.size(journal);
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            StoredState.Snapshot snap = store.replay();
            store.compact(snap);
            long after = Files.size(journal);
            long lines = Files.readString(journal).lines().count();
            log.info("[store-compact] {} 字节 -> {} 字节, 行数={}", before, after, lines);
            assertEquals(1, lines, "压缩后应为单行快照");
            assertTrue(after < before);
            // 压缩后仍可继续追加并重放
            store.appendBatch(List.of(StoredState.Change.addPending("c1", msg(11))));
        }
        try (GatewayStore store = new LocalFileGatewayStore(dir)) {
            StoredState.Snapshot snap = store.replay();
            assertEquals(11, snap.sessions().get("c1").pendingAck().size());
            assertEquals(11L, snap.maxMessageId());
        }
    }
}
