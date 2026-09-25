package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

import java.nio.charset.StandardCharsets;

/**
 * 一次发布请求。
 */
public record PublishRequest(String topic, Qos qos, byte[] payload) {
    public PublishRequest {
        if (topic == null) {
            throw new IllegalArgumentException("topic");
        }
        if (qos == null) {
            throw new IllegalArgumentException("qos");
        }
        payload = payload == null ? new byte[0] : payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public static PublishRequest of(String topic, Qos qos, String text) {
        return new PublishRequest(topic, qos, text.getBytes(StandardCharsets.UTF_8));
    }
}
