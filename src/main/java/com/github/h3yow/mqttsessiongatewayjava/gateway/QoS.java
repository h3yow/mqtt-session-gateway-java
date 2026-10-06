package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 投递服务质量等级。
 */
public enum QoS {
    /** 至多一次：投递即完成，无需确认。 */
    AT_MOST_ONCE,
    /** 至少一次：投递后需确认，未确认将在重连后重投。 */
    AT_LEAST_ONCE;

    /**
     * 发布方与订阅方 QoS 协商：取两者中较低的等级。
     * 任一方只保证至多一次，则整条投递按至多一次处理。
     */
    public static QoS negotiate(QoS publishQos, QoS subscribeQos) {
        return ordinalLower(publishQos, subscribeQos) ? publishQos : subscribeQos;
    }

    private static boolean ordinalLower(QoS a, QoS b) {
        return a.ordinal() < b.ordinal();
    }
}
