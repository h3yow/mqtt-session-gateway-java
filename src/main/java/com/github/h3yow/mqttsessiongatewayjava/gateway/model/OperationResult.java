package com.github.h3yow.mqttsessiongatewayjava.gateway.model;

/**
 * 通用操作结果（订阅 / 连接 / 断开 / 确认）。
 */
public record OperationResult(boolean accepted, RejectReason reason, String detail) {
}
