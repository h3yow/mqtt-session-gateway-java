package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * 所有被网关拒绝的操作原因。不同的失败场景必须映射到可区分的枚举值。
 */
public enum RejectReason {
    // ---- 主题 / 过滤器相关 ----
    EMPTY_TOPIC,
    ILLEGAL_LEVEL_CHARACTER,
    HASH_NOT_AT_END,
    HASH_NOT_ALONE_IN_LEVEL,
    EMPTY_FILTER,

    // ---- 会话 / 订阅相关 ----
    UNKNOWN_CLIENT,
    NOT_SUBSCRIBED,

    // ---- 确认相关 ----
    PACKET_NOT_IN_FLIGHT,
    DUPLICATE_ACK,
    QOS0_DOES_NOT_ACK,

    // ---- 容量 / 背压相关 ----
    BACKLOG_FULL,
    BACKPRESSURE_TIMEOUT
}
