package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.util.Optional;

/**
 * 网关持久状态存储：负责落盘与加载快照。
 */
public interface GatewayStateStore {

    /** 加载磁盘上的快照；尚无快照（首次启动）时返回空。 */
    Optional<GatewaySnapshot> load();

    /** 原子写入快照（先写临时文件再 rename，落盘后返回）。 */
    void save(GatewaySnapshot snapshot);
}
