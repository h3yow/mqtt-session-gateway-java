package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * 一条订阅：主题过滤器 + 最大 QoS。
 */
public record Subscription(String filter, Qos qos) {
}
