package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * 进入某客户端投递管道的消息（已分配 per-client packetId）。
 */
public record QueuedMessage(
        long packetId,
        String topic,
        byte[] payload,
        Qos qos,
        boolean redelivery,
        long enqueuedAtMillis) {
}
