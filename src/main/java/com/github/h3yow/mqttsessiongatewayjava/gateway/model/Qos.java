package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * MQTT 风格的服务质量等级。
 */
public enum Qos {
    /** 至多一次：投递即忘，不做确认与重传。 */
    AT_MOST_ONCE,
    /** 至少一次：需要 PUBACK 确认，未确认会重传。 */
    AT_LEAST_ONCE
}
