package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 一条订阅：客户端 id + 主题过滤器 + 该订阅的 QoS。
 */
public record Subscription(String clientId, TopicFilter filter, QoS qos) {
}
