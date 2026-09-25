package com.github.h3yow.mqttsessiongatewayjava.gateway.delivery;

import com.github.h3yow.mqttsessiongatewayjava.gateway.config.GatewayProperties;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import com.github.h3yow.mqttsessiongatewayjava.gateway.model.QueuedMessage;
import com.github.h3yow.mqttsessiongatewayjava.gateway.session.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 每个在线客户端独占一个 worker 线程消费 pipeline。
 *
 * <ul>
 *   <li>设备处理器调用运行在独立单线程执行器上，并以 {@code deliveryTimeout}
 *       施加超时：慢设备只会拖慢它自己的 worker，不占用发布线程，也不影响其他客户端。</li>
 *   <li>QoS1 成功后登记 inflight 等待 PUBACK；QoS0 成功即完成，失败按至多一次放弃。</li>
 *   <li>QoS1 失败的消息放回队首，按 {@code retryInterval} 退避后重投。</li>
 *   <li>停止采用“置标志 + 中断 + join”，保证断连返回前旧 worker 已退出，
 *       重连不会出现两个 worker 同时消费同一管道；不向业务管道投递哨兵。</li>
 * </ul>
 */
public class DeliveryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DeliveryDispatcher.class);

    private final GatewayProperties properties;
    private final DeviceDeliveryHandler handler;
    private final Map<String, WorkerControl> workers = new ConcurrentHashMap<>();
    private volatile boolean shutdown;

    public DeliveryDispatcher(GatewayProperties properties, DeviceDeliveryHandler handler) {
        this.properties = properties;
        this.handler = handler;
    }

    /**
     * 启动某客户端的 worker（幂等：已存在则不重复启动）。
     */
    public void start(ClientSession session) {
        if (shutdown) {
            return;
        }
        String clientId = session.clientId();
        WorkerControl fresh = new WorkerControl(session);
        WorkerControl existing = workers.putIfAbsent(clientId, fresh);
        if (existing != null) {
            return;
        }
        Thread t = new Thread(() -> runLoop(fresh), "gateway-delivery-" + clientId);
        t.setDaemon(true);
        fresh.thread = t;
        t.start();
        log.debug("投递 worker 已启动: client={}", clientId);
    }

    /**
     * 停止某客户端的 worker（断连时）。幂等；等待旧 worker 退出后再返回。
     */
    public void stop(String clientId) {
        WorkerControl ctl = workers.remove(clientId);
        if (ctl != null) {
            ctl.running = false;
            Thread t = ctl.thread;
            if (t != null) {
                t.interrupt();
                try {
                    // 最多等待一个投递超时周期 + 余量，避免异常情况下永久挂起
                    t.join(properties.getDeliveryTimeout().toMillis() + 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void shutdown() {
        shutdown = true;
        workers.keySet().forEach(this::stop);
    }

    private void runLoop(WorkerControl ctl) {
        ClientSession session = ctl.session;
        String clientId = session.clientId();
        while (ctl.running && !shutdown) {
            QueuedMessage msg;
            try {
                msg = session.pipeline().poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (msg == null) {
                continue;
            }
            boolean ok = attemptWithTimeout(clientId, msg);
            if (!ctl.running || !session.online()) {
                // 投递过程中断连：失败消息放回队首（不会被清空），
                // 成功的 QoS1 消息由断连流程从 inflight 回灌，状态不丢。
                if (!ok) {
                    requeueAtHead(session, msg);
                }
                break;
            }
            if (ok) {
                if (msg.qos() == Qos.AT_LEAST_ONCE) {
                    // 与断连回灌互斥：只有仍在线才登记 inflight；
                    // 若断连流程已先执行，消息改放回队首（断连流程随后会回灌 inflight）。
                    session.completionLock().lock();
                    try {
                        if (session.online() && ctl.running) {
                            session.inflight().put(msg.packetId(), msg);
                        } else {
                            requeueAtHead(session, msg);
                        }
                    } finally {
                        session.completionLock().unlock();
                    }
                    log.debug("投递成功等待确认: client={} packetId={} redelivery={}",
                            clientId, msg.packetId(), msg.redelivery());
                } else {
                    session.addDeliveredQos0(msg.packetId());
                    log.debug("QoS0 投递完成: client={} topic={}", clientId, msg.topic());
                }
            } else {
                if (msg.qos() == Qos.AT_MOST_ONCE) {
                    // 至多一次：失败即放弃，仅记录日志（不重试、不积压）
                    log.warn("QoS0 投递失败，按至多一次语义放弃: client={} topic={}",
                            clientId, msg.topic());
                } else {
                    requeueAtHead(session, msg);
                    sleepBeforeRetry();
                }
            }
        }
        log.debug("投递 worker 退出: client={}", clientId);
    }

    /**
     * 把消息放回有界管道队首（投递失败重试 / 断连瞬间回灌）。
     * 该消息此前占着一个槽位且刚被 poll 出来，正常情况下立即可入；
     * 若遇到容量不足则阻塞等待空位，保证消息不丢。
     */
    private static void requeueAtHead(ClientSession session, QueuedMessage msg) {
        try {
            session.pipeline().putFront(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean attemptWithTimeout(String clientId, QueuedMessage msg) {
        ExecutorService one = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "gateway-handler-" + clientId + "-" + msg.packetId());
            t.setDaemon(true);
            return t;
        });
        try {
            Future<Boolean> future = one.submit(() -> handler.deliver(clientId, msg));
            return future.get(properties.getDeliveryTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("投递超时（{}ms），本次判定失败: client={} packetId={}",
                    properties.getDeliveryTimeout().toMillis(), clientId, msg.packetId());
            return false;
        } catch (Exception e) {
            log.warn("投递异常，本次判定失败: client={} packetId={} cause={}",
                    clientId, msg.packetId(), e.toString());
            return false;
        } finally {
            one.shutdownNow();
        }
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(properties.getRetryInterval().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class WorkerControl {
        final ClientSession session;
        volatile boolean running = true;
        volatile Thread thread;

        WorkerControl(ClientSession session) {
            this.session = session;
        }
    }
}
