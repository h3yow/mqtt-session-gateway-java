# MQTT 风格设备消息网关（进程内模拟）

一个不依赖任何外部 MQTT Broker / 数据库的本地可验证网关：用进程内的连接表、订阅表与
投递管道模拟设备接入，覆盖主题路由、QoS 语义、持久会话、离线积压、容量背压与并发场景。

- Java 21 + Spring Boot 3.4，构建 Maven
- 持久化：自带的追加式 WAL 文件（`force(true)` 落盘 + 启动重放压缩），无需外部服务
- 设备投递：`DeviceDeliveryHandler` 接口，默认实现只打日志；测试注入可控实现模拟慢/故障设备

---

## 1. 主题匹配规则

主题与过滤器均以 `/` 分隔为层级；允许前导、结尾与连续斜杠（产生的**空层级也是合法层级**）。

| 符号 | 含义 | 规则 |
|------|------|------|
| 普通层级 | 精确相等 | `a/b/c` 只匹配 `a/b/c` |
| `+` | 单层通配符 | 必须**独占整个层级**，匹配恰好 1 个层级（含空层级），如 `a/+/c` 匹配 `a/x/c`、`a//c`，不匹配 `a/b/x` |
| `#` | 多层通配符 | 必须独占层级且**只能位于最后一层**，匹配其后 0 个或多个层级，如 `a/#` 匹配 `a`、`a/`、`a/b/c` |

示例：

- `sport/tennis/+` 匹配 `sport/tennis/player1`，不匹配 `sport/tennis` 或 `sport/tennis/a/b`
- `sport/#` 匹配 `sport`、`sport/`、`sport/tennis/a/b`
- 发布主题中出现 `+` 或 `#` 一律拒绝

非法过滤器 / 主题的拒绝原因（`RejectReason`）严格可区分：

| 原因 | 触发条件 |
|------|----------|
| `EMPTY_FILTER` | 过滤器为 null 或空串 |
| `EMPTY_TOPIC` | 发布主题为 null 或空串 |
| `HASH_NOT_AT_END` | `#` 出现在非最后一层，如 `a/#/b` |
| `HASH_NOT_ALONE_IN_LEVEL` | `#` 未独占层级，如 `a/b#`、`a#/b` |
| `ILLEGAL_LEVEL_CHARACTER` | `+` 未独占层级（`a/+b`），或发布主题中含通配符 |

路由保证：消息**绝不会**投递给不匹配的订阅；同一客户端多个重叠过滤器只投递一次，
有效 QoS 取匹配订阅中的最高订阅 QoS，再与发布 QoS 取最小值（`min(pubQos, subQos)`）。

---

## 2. 会话、投递与确认语义

### 会话类型

- `connect(clientId, cleanStart=true)`：非持久（clean）会话。断连即销毁全部状态；
  同 clientId 以 cleanStart 重连会丢弃该 clientId 的历史持久状态。
- `connect(clientId, cleanStart=false)`：持久会话。订阅关系与未确认 QoS1 消息在断连后、
  网关重启后均保留；重连后自动恢复并继续投递。
- 同一 clientId 重复连接：旧连接被顶替下线（持久状态保留语义按新连接的 cleanStart 决定）。

### 投递结论（`DeliveryDecision`，每个匹配订阅者一条，不静默）

| 结论 | 含义 |
|------|------|
| `DELIVERED` | 客户端在线，消息已进入其独占投递管道并完成投递（QoS1 随后等待确认） |
| `STORED_OFFLINE` | 持久会话离线，QoS1 消息已写入有界积压并落盘，重连/重启后投递 |
| `SKIPPED` | 明确不投递：非持久会话离线，或持久会话离线但消息为 QoS0 |
| `REJECTED` | 容量/背压原因拒绝（见第 3 节），携带可区分原因 |

### QoS 语义

- **QoS0 至多一次（`AT_MOST_ONCE`）**：投递即忘；不进 inflight、不需要确认；
  在线投递失败即放弃（仅日志）；离线不积压。对 QoS0 消息调用确认会被拒绝。
- **QoS1 至少一次（`AT_LEAST_ONCE`）**：投递成功后进入该会话 inflight 表，等待
  `acknowledge(clientId, packetId)`。未确认即断连、或网关重启，消息会以
  **同一 packetId + `redelivery=true`** 重新投递，由 per-client packetId 去重，
  保证消息不丢、且每条消息只能被确认成功一次。

### 确认（PUBACK）失败原因可区分

| 原因 | 触发条件 |
|------|----------|
| `UNKNOWN_CLIENT` | 会话不存在（或非对应连接） |
| `QOS0_DOES_NOT_ACK` | 确认的是一条 QoS0 消息（QoS0 无确认语义） |
| `PACKET_NOT_IN_FLIGHT` | packetId 从未投递给本会话（含“非对应订阅者拿别人的 packetId 来确认”） |
| `DUPLICATE_ACK` | packetId 在本会话范围内、但已经被确认过（重复确认） |

---

## 3. 容量与背压

每个客户端有独立的**有界投递管道**，容量由 `mqtt-gateway.max-backlog-per-session` 配置。
占用量 = 管道中待投消息数 + inflight 中待确认消息数。

发布路由到某订阅者时：

1. 占用量已达上限 → 立即拒绝，原因 `BACKLOG_FULL`；
2. 管道暂时满（消息正在投递途中）→ 发布线程最多等待 `mqtt-gateway.enqueue-timeout`，
   仍无空位则拒绝，原因 `BACKPRESSURE_TIMEOUT`。

两条路径都通过 `PublishResult.outcomes` 显式返回给发布方，**绝不静默丢弃**。

慢/故障设备的隔离：

- 每个在线客户端由一个独占 worker 线程消费管道，设备处理器在独立线程上被调用，
  并受 `mqtt-gateway.delivery-timeout` 约束；超时或抛异常只判该消息本次失败，
  QoS1 放回**队首**按 `mqtt-gateway.retry-interval` 退避重试，QoS0 放弃。
- 发布线程只做路由与带超时入队，从不执行设备 IO，因此**一个慢客户端不会阻塞发布线程，
  也不会影响其他客户端**。

### 配置项一览（`application.properties`）

| 键 | 默认值 | 说明 |
|----|--------|------|
| `mqtt-gateway.persistence-enabled` | `true` | 是否启用 WAL 持久化，false 时纯内存 |
| `mqtt-gateway.data-dir` | `target/gateway-data` | WAL 文件目录（`gateway.wal`） |
| `mqtt-gateway.max-backlog-per-session` | `1000` | 每会话积压容量上限（待投+待确认） |
| `mqtt-gateway.enqueue-timeout` | `2s` | 发布入队等待上限，超时 `BACKPRESSURE_TIMEOUT` |
| `mqtt-gateway.delivery-timeout` | `5s` | 单次设备投递超时 |
| `mqtt-gateway.retry-interval` | `200ms` | QoS1 投递失败重试退避 |

---

## 4. 持久化与重启恢复

状态变更在方法返回前以追加记录写入 `gateway.wal` 并 `force(true)`：

```
SESSION    <client>
SUB        <client> <filter> <qos>
UNSUB      <client> <filter>
DELSESSION <client>
QUEUE      <client> <packetId> <qos> <topic> <payloadB64>
ACKED      <client> <packetId>
```

网关构造时重放日志：重建持久会话、订阅与未确认积压（packetId 单调序列一并恢复），
随后把当前状态原子压缩重写成一份新日志。因此：

- 断连期间发布的 QoS1 消息、已投递未确认的消息，重启后仍会重投且不重复确认；
- 已 ACK 的消息重启后不会复活。

---

## 5. 并发模型小结

- 控制面（connect/subscribe/unsubscribe/ack/disconnect）在调用方线程执行；
  每个 clientId 一把分段锁，串行化该客户端的复合状态变更，不同客户端完全并行。
- publish 在调用方线程先对各会话短持锁收集匹配快照，再逐订阅者在其分段锁内入队。
- worker 与断连流程通过会话级 `completionLock` 互斥，消除“投递成功与断连同时发生”时
  inflight/管道漏归并的竞态；停止 worker 采用“置标志 + 中断 + join”，
  保证重连返回前旧 worker 已退出。

---

## 6. 本地验证方法

无需任何外部服务（无 Broker、无数据库），JDK 21+ 与 Maven 即可。

```bash
# 构建并运行全部单元测试（19 个，含通配符边界/重复确认/离线积压/重启恢复/并发/背压）
mvn verify

# 只跑某一类
mvn test -Dtest=TopicMatcherTest
mvn test -Dtest=DeliveryAckSemanticsTest
mvn test -Dtest=PersistenceBacklogTest
mvn test -Dtest=ConcurrencyBackpressureTest

# 启动应用（默认 LoggingDeviceDeliveryHandler 会把投递打到日志）
mvn spring-boot:run
```

测试日志会打印每个用例的**输入（topic/qos/payload）、会话状态快照（订阅、pipeline、
inflight、nextPacketId）以及投递判定依据（decision / rejectReason / detail）**，
例如：

```
[离线发布1] 判定: client=dev-1 decision=STORED_OFFLINE packetId=0 detail='客户端离线，已进入持久积压'
[离线积压状态] 会话状态: SessionView[clientId=dev-1, persistent=true, online=false,
               subscriptions={alerts/#=AT_LEAST_ONCE}, pipelineSize=1, inflightSize=0, nextPacketId=2]
并发确认结论: 成功确认数=100 拒绝分类={DUPLICATE_ACK=100}
慢客户端容量: cap=10 ... BACKLOG_FULL=... BACKPRESSURE_TIMEOUT=... 单次发布最大耗时=...ms
```

### 测试覆盖索引

| 测试类 | 覆盖点 |
|--------|--------|
| `TopicMatcherTest` | `+`/`#` 边界、空层级、非法位置、非法字符、不同拒绝原因 |
| `DeliveryAckSemanticsTest` | QoS 降级、重叠订阅去重、QoS0 拒认、重复 ACK、非订阅者 ACK、未知 packetId、错误路由防护 |
| `PersistenceBacklogTest` | 离线积压、重连 redelivery、inflight 重投不重复、跨重启恢复、ACK 后不复活、cleanStart 清除 |
| `ConcurrencyBackpressureTest` | 并发发布路由一致性、并发 ACK 恰好一次、并发订阅与发布一致性、容量上限/背压/慢客户端隔离 |

---

## 7. 主要代码结构

```
gateway/
├── MqttGateway.java                 # 门面：connect/subscribe/publish/ack/disconnect、路由与恢复
├── model/                           # Qos / RejectReason / DeliveryDecision 及结果记录
├── topic/TopicMatcher.java          # 过滤器校验与通配符匹配
├── session/                         # ClientSession（有界管道/inflight/分段标记）、SessionRegistry
├── delivery/
│   ├── DeviceDeliveryHandler.java   # 设备投递 SPI
│   ├── LoggingDeviceDeliveryHandler.java
│   ├── DeliveryDispatcher.java      # 每客户端独占 worker、投递超时、失败队首重试
│   └── BoundedDeliveryQueue.java    # 支持队首插入的有界阻塞队列
├── storage/                         # DurableStore / FileDurableStore(WAL) / NoopDurableStore
└── config/                          # GatewayProperties + Spring 装配
```
