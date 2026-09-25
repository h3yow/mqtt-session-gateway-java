package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 网关操作被拒绝时抛出，携带可区分的原因。
 */
public class GatewayException extends RuntimeException {

    private final RejectReason reason;

    public GatewayException(RejectReason reason, String detail) {
        super(reason + ": " + detail);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
