package com.github.h3yow.mqttsessiongatewayjava.gateway.delivery;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 默认设备处理器：把“投递到设备”记录为一条日志。
 *
 * <p>真实部署时可替换为向网络连接写出的实现；测试中会注入可控处理器
 * （阻塞/失败/计数）验证超时、背压与重试行为。
 */
public class LoggingDeviceDeliveryHandler implements DeviceDeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingDeviceDeliveryHandler.class);

    @Override
    public boolean deliver(String clientId, QueuedMessage message) {
        String body = new String(message.payload(), StandardCharsets.UTF_8);
        log.info("[device-delivery] client={} packetId={} qos={} redelivery={} topic='{}' payload='{}'",
                clientId, message.packetId(), message.qos(), message.redelivery(),
                message.topic(), body);
        return true;
    }
}
