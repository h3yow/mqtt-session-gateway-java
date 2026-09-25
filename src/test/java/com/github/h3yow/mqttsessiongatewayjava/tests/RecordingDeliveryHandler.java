package com.github.h3yow.mqttsessiongatewayjava.tests;

import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.DeviceDeliveryHandler;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可控设备处理器：记录每个客户端收到的消息，可模拟慢设备与失败设备。
 */
class RecordingDeliveryHandler implements DeviceDeliveryHandler {

    record Delivery(String clientId, long packetId, String topic,
                    boolean redelivery, String payload) {
    }

    final List<Delivery> received = new CopyOnWriteArrayList<>();
    final Map<String, AtomicInteger> perClientCount = new ConcurrentHashMap<>();

    /** 置位后所有投递阻塞，直到 release()。 */
    volatile CountDownLatch blockLatch;
    /** 置位后投递返回 false（模拟设备故障）。 */
    volatile boolean failAll;

    @Override
    public boolean deliver(String clientId, QueuedMessage message) throws Exception {
        if (blockLatch != null) {
            blockLatch.await(10, TimeUnit.SECONDS);
        }
        if (failAll) {
            return false;
        }
        received.add(new Delivery(clientId, message.packetId(), message.topic(),
                message.redelivery(), new String(message.payload())));
        perClientCount.computeIfAbsent(clientId, k -> new AtomicInteger()).incrementAndGet();
        return true;
    }

    void block() {
        blockLatch = new CountDownLatch(1);
    }

    void release() {
        if (blockLatch != null) {
            blockLatch.countDown();
        }
    }

    List<Delivery> of(String clientId) {
        return received.stream().filter(d -> d.clientId().equals(clientId)).toList();
    }
}
