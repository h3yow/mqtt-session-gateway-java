package com.github.h3yow.mqttsessiongatewayjava.gateway;

/**
 * 一条待路由的发布消息。
 *
 * @param id      网关内全局唯一消息 id
 * @param topic   发布主题（不含通配符）
 * @param payload 消息体
 */
public record Message(long id, String topic, String payload) {
}
