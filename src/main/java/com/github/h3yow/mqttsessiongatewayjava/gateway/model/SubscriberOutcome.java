package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * 发布路由时，针对单个订阅客户端的投递结论。
 */
public record SubscriberOutcome(
        String clientId,
        DeliveryDecision decision,
        Long packetId,
        RejectReason rejectReason,
        String detail) {
}
