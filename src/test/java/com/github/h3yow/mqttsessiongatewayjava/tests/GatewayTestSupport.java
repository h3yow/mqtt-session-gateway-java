package com.github.h3yow.mqttsessiongatewayjava.tests;

import com.github.h3yow.mqttsessiongatewayjava.gateway.MqttGateway;
import com.github.h3yow.mqttsessiongatewayjava.gateway.config.GatewayProperties;
import com.github.h3yow.mqttsessiongatewayjava.gateway.delivery.DeviceDeliveryHandler;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishRequest;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.PublishResult;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.SubscriberOutcome;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.DurableStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.FileDurableStore;
import com.github.h3yow.mqttsessiongatewayjava.gateway.storage.NoopDurableStore;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * 测试辅助：构造网关、打印输入/会话状态/投递判定、轮询等待异步状态。
 */
final class GatewayTestSupport {

    private static final Logger log = LoggerFactory.getLogger("gateway-test");

    private GatewayTestSupport() {
    }

    static GatewayProperties properties(Path dataDir, int maxBacklog) {
        GatewayProperties p = new GatewayProperties();
        p.setPersistenceEnabled(dataDir != null);
        if (dataDir != null) {
            p.setDataDir(dataDir.toString());
        }
        p.setMaxBacklogPerSession(maxBacklog);
        p.setEnqueueTimeout(Duration.ofSeconds(1));
        p.setDeliveryTimeout(Duration.ofSeconds(1));
        p.setRetryInterval(Duration.ofMillis(20));
        return p;
    }

    static MqttGateway gateway(GatewayProperties p, DeviceDeliveryHandler handler) {
        DurableStore store = p.isPersistenceEnabled()
                ? new FileDurableStore(Path.of(p.getDataDir()))
                : new NoopDurableStore();
        return new MqttGateway(p, store, handler);
    }

    static void logPublish(String label, PublishRequest req, PublishResult result) {
        log.info("[{}] 输入: topic='{}' qos={} payload='{}'",
                label, req.topic(), req.qos(), new String(req.payload()));
        for (SubscriberOutcome o : result.outcomes()) {
            log.info("[{}] 判定: client={} decision={} packetId={} reason={} detail='{}'",
                    label, o.clientId(), o.decision(), o.packetId(),
                    o.rejectReason(), o.detail());
        }
    }

    static void logView(String label, MqttGateway gw, String clientId) {
        log.info("[{}] 会话状态: {}", label, gw.sessionView(clientId));
    }

    static boolean await(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }
}
