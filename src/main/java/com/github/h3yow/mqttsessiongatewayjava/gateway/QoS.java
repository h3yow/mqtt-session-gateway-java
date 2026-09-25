package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 投递服务质量等级。
 */
public enum QoS {
    /** 至多一次：投递即完成，无需确认。 */
    AT_MOST_ONCE,
    /** 至少一次：投递后需确认，未确认将在重连后重投。 */
    AT_LEAST_ONCE
}
