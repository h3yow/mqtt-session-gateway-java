package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

import java.util.List;

/**
 * 一次发布的路由结论：每个匹配订阅者一条 outcome。
 */
public record PublishResult(String topic, Qos qos, List<SubscriberOutcome> outcomes) {
}
