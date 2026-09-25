package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * 网关针对某一个订阅者的投递判定结论。
 */
public enum DeliveryDecision {
    /** 已在线完成投递。 */
    DELIVERED,
    /** 客户端离线，消息已写入持久积压。 */
    STORED_OFFLINE,
    /** 该会话不匹配，未投递。 */
    SKIPPED,
    /** 因容量 / 背压原因拒绝。 */
    REJECTED
}
