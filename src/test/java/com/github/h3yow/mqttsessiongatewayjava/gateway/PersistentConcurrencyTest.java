package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 启用本地持久化时的并发一致性：文件追加在网关单把锁内串行，
 * 多线程发布/确认后关闭网关，再以新实例重放，统计必须与关闭前完全一致。
 */
class PersistentConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(PersistentConcurrencyTest.class);

    private static final int CLIENTS = 4;
    private static final int THREADS = 8;
    private static final int PER_THREAD = 25;
    private static final int TOTAL = THREADS * PER_THREAD; // 200

    @Test
    void persistedConcurrentPublishesReplayToSameState(@TempDir Path dir) throws Exception {
        try (Gateway gw = newGateway(dir)) {
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(THREADS);
            ConcurrentLinkedQueue<List<String>> results = new ConcurrentLinkedQueue<>();
            for (int t = 0; t < THREADS; t++) {
                pool.submit(() -> {
                    try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    for (int k = 0; k < PER_THREAD; k++) {
                        results.add(gw.publish("t/x", "m", QoS.AT_LEAST_ONCE));
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            assertEquals(TOTAL, results.size());
            for (List<String> r : results) {
                assertEquals(CLIENTS, r.size());
            }
            for (int i = 0; i < CLIENTS; i++) {
                assertEquals(TOTAL, gw.pendingAckCount("c" + i));
            }
            assertEquals((long) CLIENTS * TOTAL, gw.inFlightCount());
            log.info("[persist-conc] 发布完成: total={} inFlight={}，关闭后重放校验", TOTAL, gw.inFlightCount());
        }

        // 全新实例重放：每个持久会话待确认数与在途总量必须一致（不丢、不重）
        try (Gateway gw2 = newGateway(dir)) {
            for (int i = 0; i < CLIENTS; i++) {
                assertEquals(TOTAL, gw2.pendingAckCount("c" + i), "c" + i + " 重放后待确认数不一致");
            }
            assertEquals((long) CLIENTS * TOTAL, gw2.inFlightCount(), "重放后在途总量不一致");
            log.info("[persist-conc] 重放一致: inFlight={} ({} 次 fsync 批次后无丢失/重复)",
                    gw2.inFlightCount(), TOTAL);
        }
    }

    private Gateway newGateway(Path dir) {
        GatewayConfig config = new GatewayConfig();
        config.setDataDir(dir.toString());
        Gateway gw = new Gateway(config);
        for (int i = 0; i < CLIENTS; i++) {
            gw.connect("c" + i, true);
            gw.subscribe("c" + i, "t/#", QoS.AT_LEAST_ONCE);
        }
        return gw;
    }
}
