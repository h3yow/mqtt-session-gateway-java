package com.github.h3yow.mqttsessiongatewayjava.gateway.config;

import com.github.h3yow.mqttsessiongatewayjava.gateway.MqttGateway;
import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.DeviceDeliveryHandler;
import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.LoggingDeviceDeliveryHandler;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.DurableStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.FileDurableStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.NoopDurableStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 网关 Spring 装配：默认提供基于文件 WAL 的持久化与日志型设备处理器，
 * 业务/测试可用同类型 Bean 覆盖。
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    public MqttGateway mqttGateway(GatewayProperties properties,
                                   org.springframework.beans.factory.ObjectProvider<DeviceDeliveryHandler> handlerProvider) {
        DurableStore store = properties.isPersistenceEnabled()
                ? new FileDurableStore(Path.of(properties.getDataDir()))
                : new NoopDurableStore();
        DeviceDeliveryHandler handler = handlerProvider.getIfAvailable(
                LoggingDeviceDeliveryHandler::new);
        return new MqttGateway(properties, store, handler);
    }
}
