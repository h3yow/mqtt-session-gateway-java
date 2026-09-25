package com.github.h3yow.mqttsessiongatewayjava.tests;

import com.github.h3yow.mqttsessiongatewayjava.gateway.MqttGateway;
import com.github.h3yow.mqttsessiongatewayjava.gateway.config.GatewayProperties;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.DeliveryDecision;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.OperationResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishRequest;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.SubscriberOutcome;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发场景：
 * 多线程同时发布同主题，路由结果必须一致（每个发布者看到相同匹配集合）；
 * 多线程同时确认同一批 packetId，每条消息恰好被确认成功一次；
 * 容量上限触发 BACKLOG_FULL；慢客户端触发 BACKPRESSURE_TIMEOUT，
 * 且不阻塞其他客户端的投递。
 */
class ConcurrencyBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyBackpressureTest.class);

    @Test
    void concurrentPublishSameTopicHasConsistentRouting(@org.junit.jupiter.api.io.TempDir Path dir)
            throws Exception {
        GatewayProperties props = GatewayTestSupport.properties(dir, 100_000);
        props.setEnqueueTimeout(Duration.ofSeconds(5));
        MqttGateway gw = GatewayTestSupport.gateway(props, new RecordingDeliveryHandler());

        gw.connect("sub-1", false);
        gw.connect("sub-2", false);
        gw.connect("sub-3", false);
        gw.subscribe("sub-1", "room/+/temp", Qos.AT_MOST_ONCE);
        gw.subscribe("sub-2", "room/#", Qos.AT_MOST_ONCE);
        gw.subscribe("sub-3", "other/#", Qos.AT_MOST_ONCE);

        int publishers = 12;
        int perPublisher = 200;
        ExecutorService pool = Executors.newFixedThreadPool(publishers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<List<String>> routedSets = new ConcurrentLinkedQueue<>();
        AtomicInteger rejected = new AtomicInteger();

        for (int p = 0; p < publishers; p++) {
            final int publisherId = p;
            pool.submit(() -> {
                awaitQuiet(start);
                for (int i = 0; i < perPublisher; i++) {
                    PublishRequest req = PublishRequest.of(
                            "room/" + (publisherId % 4) + "/temp",
                            Qos.AT_MOST_ONCE, "m-" + publisherId + "-" + i);
                    PublishResult result = gw.publish(req);
                    List<String> ids = result.outcomes().stream()
                            .map(SubscriberOutcome::clientId).sorted()
                            .collect(Collectors.toList());
                    routedSets.add(ids);
                    result.outcomes().forEach(o -> {
                        if (o.decision() == DeliveryDecision.REJECTED) {
                            rejected.incrementAndGet();
                        }
                    });
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long distinct = routedSets.stream().distinct().count();
        log.info("并发发布: 总发布={} 不同路由集合数={} 拒绝数={} 示例={}",
                routedSets.size(), distinct, rejected.get(),
                routedSets.stream().findFirst().orElse(List.of()));
        assertEquals(1, distinct, "同主题的并发路由结果必须一致");
        assertEquals(List.of("sub-1", "sub-2"), routedSets.peek());
        assertEquals(0, rejected.get());
        gw.shutdown();
    }

    @Test
    void concurrentAckEachPacketAcceptedExactlyOnce(@org.junit.jupiter.api.io.TempDir Path dir)
            throws Exception {
        GatewayProperties props = GatewayTestSupport.properties(dir, 100_000);
        RecordingDeliveryHandler handler = new RecordingDeliveryHandler();
        MqttGateway gw = GatewayTestSupport.gateway(props, handler);
        gw.connect("dev", false);
        gw.subscribe("dev", "q/+", Qos.AT_LEAST_ONCE);

        int messages = 100;
        List<Long> packetIds = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
            PublishResult r = gw.publish(PublishRequest.of("q/" + i, Qos.AT_LEAST_ONCE, "x"));
            packetIds.add(r.outcomes().get(0).packetId());
        }
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(10),
                () -> handler.of("dev").size() == messages));
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(5),
                () -> gw.sessionView("dev").inflightSize() == messages));
        GatewayTestSupport.logView("并发确认前(inflight=100)", gw, "dev");

        int ackers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(ackers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        ConcurrentLinkedQueue<RejectReason> rejectReasons = new ConcurrentLinkedQueue<>();
        for (int w = 0; w < ackers; w++) {
            final int worker = w;
            pool.submit(() -> {
                awaitQuiet(start);
                for (int i = 0; i < messages; i++) {
                    if ((i % ackers) == worker) {
                        OperationResult res = gw.acknowledge("dev", packetIds.get(i));
                        if (res.accepted()) {
                            accepted.incrementAndGet();
                        } else {
                            rejectReasons.add(res.reason());
                        }
                    }
                }
            });
        }
        // 抢确认线程：对全部消息再确认一遍，制造重复确认竞争
        pool.submit(() -> {
            awaitQuiet(start);
            for (long pid : packetIds) {
                OperationResult res = gw.acknowledge("dev", pid);
                if (res.accepted()) {
                    accepted.incrementAndGet();
                } else {
                    rejectReasons.add(res.reason());
                }
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        log.info("并发确认结论: 成功确认数={} 拒绝分类={}",
                accepted,
                rejectReasons.stream().collect(Collectors.groupingBy(
                        r -> r, Collectors.counting())));
        assertEquals(messages, accepted.get(), "每条消息必须且只能确认成功一次");
        Map<RejectReason, Long> counts = rejectReasons.stream()
                .collect(Collectors.groupingBy(r -> r, Collectors.counting()));
        assertEquals(1, counts.size());
        assertTrue(counts.containsKey(RejectReason.DUPLICATE_ACK));
        assertEquals(0, gw.sessionView("dev").inflightSize());
        gw.shutdown();
    }

    @Test
    void backlogCapRejectsExplicitlyAndSlowClientDoesNotBlockOthers(
            @org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
        int cap = 10;
        GatewayProperties props = GatewayTestSupport.properties(dir, cap);
        props.setEnqueueTimeout(Duration.ofMillis(200));
        props.setDeliveryTimeout(Duration.ofSeconds(10));

        // 初始放行 1 条的闸门：先确认慢客户端确实进入了“投递中”状态，再阻塞后续全部投递
        CountDownLatch enteredDelivery = new CountDownLatch(1);
        CountDownLatch releaseDelivery = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();
        RecordingDeliveryHandler handler = new RecordingDeliveryHandler() {
            @Override
            public boolean deliver(String clientId,
                                   com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage message)
                    throws Exception {
                if ("slow".equals(clientId)) {
                    if (entered.incrementAndGet() == 1) {
                        enteredDelivery.countDown();
                    }
                    releaseDelivery.await(10, TimeUnit.SECONDS);
                    return super.deliver(clientId, message);
                }
                return super.deliver(clientId, message);
            }
        };
        MqttGateway gw = GatewayTestSupport.gateway(props, handler);

        gw.connect("slow", false);
        gw.subscribe("slow", "load/#", Qos.AT_LEAST_ONCE);
        gw.connect("fast", false);
        gw.subscribe("fast", "fast/#", Qos.AT_LEAST_ONCE);

        // 第一条把 worker 钉在投递中
        PublishResult first = gw.publish(PublishRequest.of("load/0", Qos.AT_LEAST_ONCE, "v"));
        assertEquals(DeliveryDecision.DELIVERED, first.outcomes().get(0).decision());
        assertTrue(enteredDelivery.await(5, TimeUnit.SECONDS),
                "第一条消息必须已进入设备投递");

        // 此时 worker 被钉住，剩余消息只能堆满管道
        int extra = cap + 5;
        int backlogFull = 0;
        int backpressureTimeout = 0;
        int enqueued = 0;
        long maxPublishMillis = 0;
        for (int i = 1; i <= extra; i++) {
            long t0 = System.nanoTime();
            PublishResult r = gw.publish(PublishRequest.of("load/" + i, Qos.AT_LEAST_ONCE, "v"));
            long costMs = (System.nanoTime() - t0) / 1_000_000;
            maxPublishMillis = Math.max(maxPublishMillis, costMs);
            SubscriberOutcome o = r.outcomes().get(0);
            if (o.rejectReason() == RejectReason.BACKLOG_FULL) {
                backlogFull++;
            } else if (o.rejectReason() == RejectReason.BACKPRESSURE_TIMEOUT) {
                backpressureTimeout++;
            } else {
                enqueued++;
            }
        }
        log.info("慢客户端容量: cap={} 第一条在投递中, 后续入队={} BACKLOG_FULL={} BACKPRESSURE_TIMEOUT={} 单次发布最大耗时={}ms",
                cap, enqueued, backlogFull, backpressureTimeout, maxPublishMillis);
        assertTrue(backlogFull + backpressureTimeout > 0,
                "超过容量必须有明确拒绝，不得静默丢弃");
        assertEquals(extra, enqueued + backlogFull + backpressureTimeout,
                "每条发布都必须有明确结论");
        // 发布线程绝不能被慢设备无限拖住：最长耗时约等于一个背压超时
        assertTrue(maxPublishMillis <= 1_000,
                "慢客户端不得长时间阻塞发布线程，实际最大耗时 " + maxPublishMillis + "ms");

        // 慢客户端被打满期间，快速客户端必须照常快速投递
        long t0 = System.nanoTime();
        PublishResult fastResult = gw.publish(PublishRequest.of("fast/ping",
                Qos.AT_LEAST_ONCE, "p"));
        long fastElapsedMs = (System.nanoTime() - t0) / 1_000_000;
        log.info("快速客户端判定: {} 耗时={}ms", fastResult.outcomes(), fastElapsedMs);
        assertEquals(DeliveryDecision.DELIVERED, fastResult.outcomes().get(0).decision());
        assertTrue(fastElapsedMs < 500,
                "慢客户端不得阻塞其他客户端，实际耗时 " + fastElapsedMs + "ms");
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(5),
                () -> handler.of("fast").stream().anyMatch(d -> d.topic().equals("fast/ping"))));

        // 放行慢客户端：积压被逐条投递，新消息也能正常进入（不丢失已入队消息）
        releaseDelivery.countDown();
        int expectedDelivered = 1 + enqueued;
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(10),
                () -> handler.of("slow").size() == expectedDelivered),
                "放行后已接收容量内全部消息，期望=" + expectedDelivered
                        + " 实际=" + handler.of("slow").size());
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(5),
                () -> gw.sessionView("slow").inflightSize() == expectedDelivered));
        log.info("放行后慢客户端收到 {} 条，会话={}", handler.of("slow").size(),
                gw.sessionView("slow"));

        // 全部确认后释放容量（inflight 计入积压占用），新消息即可正常入队投递
        for (var d : handler.of("slow")) {
            OperationResult ack = gw.acknowledge("slow", d.packetId());
            assertTrue(ack.accepted(), "确认容量内消息应成功: " + ack);
        }
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(3),
                () -> gw.sessionView("slow").inflightSize() == 0));

        // 确认清空后新发消息不再被拒
        PublishResult after = gw.publish(PublishRequest.of("load/after",
                Qos.AT_LEAST_ONCE, "a"));
        assertEquals(DeliveryDecision.DELIVERED, after.outcomes().get(0).decision());
        assertTrue(GatewayTestSupport.await(Duration.ofSeconds(5),
                () -> handler.of("slow").stream().anyMatch(d -> d.topic().equals("load/after"))));
        GatewayTestSupport.logView("恢复后慢客户端", gw, "slow");
        gw.shutdown();
    }

    @Test
    void concurrentSubscribeAndPublishNeverCorruptsRouting(@org.junit.jupiter.api.io.TempDir Path dir)
            throws Exception {
        GatewayProperties props = GatewayTestSupport.properties(dir, 100_000);
        props.setEnqueueTimeout(Duration.ofSeconds(5));
        MqttGateway gw = GatewayTestSupport.gateway(props, new RecordingDeliveryHandler());

        // 20 个客户端并发接入+订阅 20 个不同过滤器，同时 8 个发布线程持续发同主题
        int clients = 20;
        for (int i = 0; i < clients; i++) {
            gw.connect("c-" + i, true);
        }
        ExecutorService pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger matchedDeliveries = new AtomicInteger();

        for (int i = 0; i < clients; i++) {
            final int idx = i;
            pool.submit(() -> {
                awaitQuiet(start);
                // 一半订阅精确相关过滤器，一半订阅不相关过滤器
                String filter = (idx % 2 == 0) ? ("evt/part-" + idx % 4) : "other/#";
                OperationResult r = gw.subscribe("c-" + idx, filter, Qos.AT_MOST_ONCE);
                if (!r.accepted()) {
                    errors.incrementAndGet();
                }
            });
        }
        for (int p = 0; p < 8; p++) {
            pool.submit(() -> {
                awaitQuiet(start);
                for (int i = 0; i < 100; i++) {
                    final int part = i % 4;
                    final String topic = "evt/part-" + part;
                    PublishResult r = gw.publish(PublishRequest.of(topic, Qos.AT_MOST_ONCE, "x"));
                    // 不变式：每个返回的订阅者必须确实订阅了匹配过滤器
                    for (SubscriberOutcome o : r.outcomes()) {
                        MqttGateway.SessionView view = gw.sessionView(o.clientId());
                        boolean reallyMatches = view != null && view.subscriptions().keySet()
                                .stream().anyMatch(f -> com.github.h3yow.mqttsessiongatewayjava
                                        .gateway.topic.TopicMatcher.matches(f, topic));
                        if (!reallyMatches) {
                            errors.incrementAndGet();
                        }
                        if (o.decision() == DeliveryDecision.DELIVERED) {
                            matchedDeliveries.incrementAndGet();
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        log.info("并发订阅+发布: 路由判定={} 一致性错误={}", matchedDeliveries.get(), errors.get());
        assertEquals(0, errors.get(), "并发订阅/发布期间路由不得投给不匹配订阅，订阅不得损坏");
        gw.shutdown();
    }

    private static void awaitQuiet(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
