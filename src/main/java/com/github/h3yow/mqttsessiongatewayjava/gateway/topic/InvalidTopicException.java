package com.github.h3yow.mqttsessiongatewayjava.gateway.topic;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.RejectReason;

/**
 * 非法发布主题 / 主题过滤器。
 */
public class InvalidTopicException extends RuntimeException {
    private final RejectReason reason;

    public InvalidTopicException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
