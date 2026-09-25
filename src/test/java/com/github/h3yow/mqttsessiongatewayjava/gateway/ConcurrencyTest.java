package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发场景：多线程同时发布/确认时路由结果一致、确认恰好一次、
 * 慢客户端只触发明确拒绝而不阻塞网关其余部分。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    private static final int CLIENTS = 8;
    private static final int PUBLISHER_THREADS = 16;
    private static final int PUBLISHES_PER_THREAD = 50;
    private static final int TOTAL = PUBLISHER_THREADS * PUBLISHES_PER_THREAD; // 800

    private Gateway newGatewayWithSubscribers() {
        Gateway gateway = new Gateway(new GatewayConfig());
        for (int i = 0; i < CLIENTS; i++) {
            gateway.connect("c" + i, false);
            gateway.subscribe("c" + i, "t/#", QoS.AT_LEAST_ONCE);
        }
        return gateway;
    }

    @Test
    void concurrentPublishKeepsRoutingConsistent() throws InterruptedException {
        Gateway gateway = newGatewayWithSubscribers();
        ConcurrentLinkedQueue<List<String>> routeResults = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(PUBLISHER_THREADS);
        for (int t = 0; t < PUBLISHER_THREADS; t++) {
            int thread = t;
            pool.submit(() -> {
                await(start);
                for (int k = 0; k < PUBLISHES_PER_THREAD; k++) {
                    routeResults.add(gateway.publish("t/x", "from-" + thread + "-" + k));
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // 每次发布的路由判定都必须恰好是全部 8 个订阅者（无遗漏、无重复、无不匹配者）
        assertEquals(TOTAL, routeResults.size());
        for (List<String> routed : routeResults) {
            assertEquals(CLIENTS, routed.size(), "路由结果必须包含全部订阅者: " + routed);
            assertEquals(CLIENTS, routed.stream().distinct().count(), "路由结果不得重复: " + routed);
        }
        // 每个客户端的待确认恰好等于总发布数：无丢失、无重复投递
        for (int i = 0; i < CLIENTS; i++) {
            assertEquals(TOTAL, gateway.pendingAckCount("c" + i));
        }
        assertEquals((long) CLIENTS * TOTAL, gateway.inFlightCount());
        log.info("[conc-publish] publishes={} clients={} inFlight={} 每次路由均为全部订阅者",
                TOTAL, CLIENTS, gateway.inFlightCount());
    }

    @Test
    void concurrentAcksAreExactlyOnce() throws InterruptedException {
        Gateway gateway = newGatewayWithSubscribers();
        for (int k = 0; k < TOTAL; k++) {
            gateway.publish("t/x", "m" + k); // 消息 id 恰为 1..TOTAL
        }
        // 每个客户端并发确认自己的全部待确认消息，同时混入重复确认
        AtomicInteger ackOk = new AtomicInteger();
        AtomicInteger ackDup = new AtomicInteger();
        AtomicInteger ackWrong = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < CLIENTS; i++) {
            String client = "c" + i;
            for (long id = 1; id <= TOTAL; id++) {
                long messageId = id;
                pool.submit(() -> {
                    await(start);
                    try {
                        gateway.ack(client, messageId);
                        ackOk.incrementAndGet();
                    } catch (GatewayException e) {
                        if (e.reason() == RejectReason.ACK_DUPLICATE) {
                            ackDup.incrementAndGet();
                        } else {
                            ackWrong.incrementAndGet();
                        }
                    }
                    // 并发再确认一次：必须被拒绝且原因为重复确认
                    try {
                        gateway.ack(client, messageId);
                    } catch (GatewayException e) {
                        if (e.reason() == RejectReason.ACK_DUPLICATE) {
                            ackDup.incrementAndGet();
                        } else {
                            ackWrong.incrementAndGet();
                        }
                    }
                });
            }
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        log.info("[conc-ack] ok={} dup={} wrong={} inFlight={} (expect ok={} wrong=0 inFlight=0)",
                ackOk.get(), ackDup.get(), ackWrong.get(), gateway.inFlightCount(), (long) CLIENTS * TOTAL);
        // 恰好一次：成功确认数 == 客户端数 × 消息数；不存在其他类型的失败
        assertEquals((long) CLIENTS * TOTAL, ackOk.get());
        assertEquals(0, ackWrong.get());
        assertTrue(ackDup.get() >= (long) CLIENTS * TOTAL, "每次重复确认都必须被识别为 ACK_DUPLICATE");
        assertEquals(0, gateway.inFlightCount());
        for (int i = 0; i < CLIENTS; i++) {
            assertEquals(0, gateway.pendingAckCount("c" + i));
        }
    }

    @Test
    void slowClientTriggersExplicitRejectionWithoutBlockingGateway() {
        GatewayConfig config = new GatewayConfig();
        config.setBacklogCapacity(1);
        Gateway gateway = new Gateway(config);
        // 慢客户端：持久会话、离线、积压立刻打满
        gateway.connect("slow", true);
        gateway.subscribe("slow", "busy/#", QoS.AT_MOST_ONCE);
        gateway.disconnect("slow");
        // 正常在线客户端订阅另一主题
        gateway.connect("fast", false);
        gateway.subscribe("fast", "other/#", QoS.AT_LEAST_ONCE);

        gateway.publish("busy/a", "m1"); // slow 积压 1/1
        GatewayException ex = null;
        try {
            gateway.publish("busy/b", "m2");
        } catch (GatewayException e) {
            ex = e;
        }
        log.info("[slow-client] reason={} (明确拒绝而非静默丢弃或阻塞)",
                ex == null ? null : ex.reason());
        assertTrue(ex != null && ex.reason() == RejectReason.BACKLOG_CAPACITY_EXCEEDED);

        // 网关其余部分不受影响：其他主题的发布与确认照常
        List<String> routed = gateway.publish("other/x", "ok");
        assertEquals(List.of("fast"), routed);
        gateway.ack("fast", 3L); // 第 3 条消息（前两条是 busy/a、被拒的 busy/b 未占 id? busy/b 在容量预检前已分配 id=2? 否：预检在 id 分配后）
        log.info("[slow-client] fast client unaffected, routed={}", routed);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
