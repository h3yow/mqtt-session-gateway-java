package com.github.h3yow.mqttsessiongatewayjava.gateway.delivery;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage;

/**
 * 设备侧投递处理器：网关把消息交给它代表“发到设备”。
 *
 * <p>处理必须在线程池 worker 中执行；实现应快速返回，
 * 慢设备通过阻塞或抛异常表达，网关负责超时/失败隔离，不影响其他客户端。
 */
@FunctionalInterface
public interface DeviceDeliveryHandler {
    /**
     * @return true 表示设备已收到；false/异常表示投递失败需要保留以便重试
     */
    boolean deliver(String clientId, QueuedMessage message) throws Exception;
}
