package com.github.h3yow.mqttsessiongatewayjava.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 容量与背压相关配置项。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayConfig {

    /** 单会话离线积压容量上限，超过即拒绝（BACKLOG_CAPACITY_EXCEEDED）。 */
    private int backlogCapacity = 1000;

    /** 全网关在途（已投递待确认）消息上限，超过即限流（BACKPRESSURE_LIMIT_EXCEEDED）。 */
    private int maxInFlight = 10000;

    /** 本地数据目录；为空时网关为纯内存模式（进程重启不恢复）。 */
    private String dataDir = "";

    public int getBacklogCapacity() {
        return backlogCapacity;
    }

    public void setBacklogCapacity(int backlogCapacity) {
        this.backlogCapacity = backlogCapacity;
    }

    public int getMaxInFlight() {
        return maxInFlight;
    }

    public void setMaxInFlight(int maxInFlight) {
        this.maxInFlight = maxInFlight;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }
}
