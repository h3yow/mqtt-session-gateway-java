package com.github.h3yow.mqttsessiongatewayjava.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 网关容量与背压相关配置。
 */
@ConfigurationProperties(prefix = "mqtt-gateway")
public class GatewayProperties {

    /** 是否启用持久化（WAL 文件）。 */
    private boolean persistenceEnabled = true;

    /** WAL 与快照存放目录。 */
    private String dataDir = "target/gateway-data";

    /** 每个持久会话离线积压的最大消息条数。 */
    private int maxBacklogPerSession = 1000;

    /** 发布线程等待慢客户端接收管道的最长时间（背压）。 */
    private Duration enqueueTimeout = Duration.ofSeconds(2);

    /** worker 调用设备处理器的最长时间，超时视为本次投递失败。 */
    private Duration deliveryTimeout = Duration.ofSeconds(5);

    /** 投递失败后的重试间隔。 */
    private Duration retryInterval = Duration.ofMillis(200);

    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    public void setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getMaxBacklogPerSession() {
        return maxBacklogPerSession;
    }

    public void setMaxBacklogPerSession(int maxBacklogPerSession) {
        this.maxBacklogPerSession = maxBacklogPerSession;
    }

    public Duration getEnqueueTimeout() {
        return enqueueTimeout;
    }

    public void setEnqueueTimeout(Duration enqueueTimeout) {
        this.enqueueTimeout = enqueueTimeout;
    }

    public Duration getDeliveryTimeout() {
        return deliveryTimeout;
    }

    public void setDeliveryTimeout(Duration deliveryTimeout) {
        this.deliveryTimeout = deliveryTimeout;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }
}
