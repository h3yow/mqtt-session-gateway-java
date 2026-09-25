package com.github.h3yow.mqttsessiongatewayjava.gateway;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 将进程内网关注册为 Spring Bean，并启用容量/背压配置项。
 */
@Configuration
@EnableConfigurationProperties(GatewayConfig.class)
public class GatewayBeanConfig {

    @Bean
    public Gateway gateway(GatewayConfig config) {
        return new Gateway(config);
    }
}
