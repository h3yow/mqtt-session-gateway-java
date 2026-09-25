package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 可区分的拒绝原因。
 */
public enum RejectReason {
    /** 主题过滤器为空或含空层级。 */
    FILTER_EMPTY_LEVEL,
    /** 通配符出现在非法位置（如 "a+/b"、"a/#/b"）。 */
    FILTER_WILDCARD_MISPLACED,
    /** 主题名非法（发布主题不得含通配符）。 */
    TOPIC_INVALID,
    /** 会话不存在（未连接且无持久会话）。 */
    SESSION_NOT_FOUND,
    /** 客户端当前离线，操作要求在线。 */
    SESSION_OFFLINE,
    /** 重复确认：该消息已被确认过。 */
    ACK_DUPLICATE,
    /** 非对应订阅者的确认：该消息不属于此客户端的待确认集合。 */
    ACK_NOT_PENDING_FOR_CLIENT,
    /** 积压队列超过容量上限。 */
    BACKLOG_CAPACITY_EXCEEDED,
    /** 背压触发：网关在途消息超过上限，发布被限流。 */
    BACKPRESSURE_LIMIT_EXCEEDED
}
