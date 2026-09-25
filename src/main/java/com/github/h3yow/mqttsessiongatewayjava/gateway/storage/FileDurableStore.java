package com.github.h3yow.mqttsessiongatewayjava.gateway.storage;

import com.github.h3yow.mqttsessiongatewayjava.gateway.model.Qos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 基于追加 WAL（write-ahead-log）的持久化实现。
 *
 * <p>每条状态变更在返回前 {@code force(true)} 落盘；启动时重放日志恢复，
 * 重放完成后立即把当前状态压缩重写为一份新日志，避免无限增长。
 *
 * <p>日志行格式（字段以 TAB 分隔，字符串字段使用 Base64-URL 编码以规避特殊字符）：
 * <pre>
 * SESSION     client
 * SUB         client filter qos(0=QOS0,1=QOS1)
 * UNSUB       client filter
 * DELSESSION  client
 * QUEUE       client packetId qos topic payloadB64
 * ACKED       client packetId
 * </pre>
 */
public class FileDurableStore implements DurableStore {

    private static final Logger log = LoggerFactory.getLogger(FileDurableStore.class);

    private final Path walFile;
    private final Object writeLock = new Object();
    private boolean closed;

    public FileDurableStore(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
            this.walFile = dataDir.resolve("gateway.wal");
        } catch (IOException e) {
            throw new UncheckedIOException("初始化持久化目录失败: " + dataDir, e);
        }
    }

    @Override
    public void recordSession(String clientId) {
        append("SESSION", enc(clientId));
    }

    @Override
    public void recordSubscription(String clientId, String filter, Qos qos) {
        append("SUB", enc(clientId), enc(filter), String.valueOf(qos.ordinal()));
    }

    @Override
    public void recordRemoveSubscription(String clientId, String filter) {
        append("UNSUB", enc(clientId), enc(filter));
    }

    @Override
    public void recordRemoveSession(String clientId) {
        append("DELSESSION", enc(clientId));
    }

    @Override
    public void recordQueued(String clientId, long packetId, String topic, Qos qos, byte[] payload) {
        append("QUEUE", enc(clientId), String.valueOf(packetId), String.valueOf(qos.ordinal()),
                enc(topic), Base64.getEncoder().encodeToString(payload));
    }

    @Override
    public void recordAcked(String clientId, long packetId) {
        append("ACKED", enc(clientId), String.valueOf(packetId));
    }

    private void append(String... fields) {
        synchronized (writeLock) {
            if (closed) {
                throw new IllegalStateException("DurableStore 已关闭");
            }
            byte[] line = (String.join("\t", fields) + System.lineSeparator())
                    .getBytes(StandardCharsets.UTF_8);
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel
                    .open(walFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                            StandardOpenOption.APPEND)) {
                ch.write(java.nio.ByteBuffer.wrap(line));
                ch.force(true);
            } catch (IOException e) {
                throw new UncheckedIOException("写入 WAL 失败", e);
            }
        }
    }

    @Override
    public Snapshot loadSnapshot() {
        // 重放过程使用的可变结构
        Map<String, Boolean> exists = new TreeMap<>();
        Map<String, LinkedHashMap<String, Qos>> subs = new TreeMap<>();
        Map<String, LinkedHashMap<Long, MsgEntry>> queued = new TreeMap<>();
        Map<String, Long> maxPacketId = new TreeMap<>();

        if (!Files.exists(walFile)) {
            return new Snapshot(List.of(), Map.of(), Map.of(), Map.of());
        }
        long lineNo = 0;
        try (BufferedReader reader = Files.newBufferedReader(walFile, StandardCharsets.UTF_8)) {
            String raw;
            while ((raw = reader.readLine()) != null) {
                lineNo++;
                if (raw.isEmpty()) {
                    continue;
                }
                String[] f = raw.split("\t", -1);
                try {
                    replay(f, exists, subs, queued, maxPacketId);
                } catch (RuntimeException e) {
                    log.warn("忽略损坏的 WAL 记录 line={}: {}", lineNo, raw, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取 WAL 失败", e);
        }

        List<String> sessions = exists.entrySet().stream()
                .filter(Map.Entry::getValue).map(Map.Entry::getKey).sorted().toList();
        Snapshot snapshot = new Snapshot(
                sessions,
                deepCopySubs(subs),
                deepCopyQueued(queued, maxPacketId),
                nextIds(maxPacketId));
        compact(snapshot);
        log.info("WAL 恢复完成: 持久会话={} 个, 恢复积压={} 条",
                sessions.size(),
                snapshot.queued().values().stream().mapToInt(List::size).sum());
        return snapshot;
    }

    private void replay(String[] f,
                        Map<String, Boolean> exists,
                        Map<String, LinkedHashMap<String, Qos>> subs,
                        Map<String, LinkedHashMap<Long, MsgEntry>> queued,
                        Map<String, Long> maxPacketId) {
        switch (f[0]) {
            case "SESSION" -> exists.put(dec(f[1]), true);
            case "DELSESSION" -> {
                String c = dec(f[1]);
                exists.put(c, false);
                subs.remove(c);
                queued.remove(c);
            }
            case "SUB" -> {
                String c = dec(f[1]);
                subs.computeIfAbsent(c, k -> new LinkedHashMap<>())
                        .put(dec(f[2]), Qos.values()[Integer.parseInt(f[3])]);
                exists.putIfAbsent(c, true);
            }
            case "UNSUB" -> {
                var m = subs.get(dec(f[1]));
                if (m != null) {
                    m.remove(dec(f[2]));
                }
            }
            case "QUEUE" -> {
                String c = dec(f[1]);
                long pid = Long.parseLong(f[2]);
                Qos qos = Qos.values()[Integer.parseInt(f[3])];
                MsgEntry e = new MsgEntry(pid, dec(f[4]), qos,
                        Base64.getDecoder().decode(f[5]));
                queued.computeIfAbsent(c, k -> new LinkedHashMap<>()).put(pid, e);
                maxPacketId.merge(c, pid, Math::max);
            }
            case "ACKED" -> {
                String c = dec(f[1]);
                long pid = Long.parseLong(f[2]);
                var m = queued.get(c);
                if (m != null) {
                    m.remove(pid);
                }
                maxPacketId.merge(c, pid, Math::max);
            }
            default -> throw new IllegalArgumentException("未知记录类型: " + f[0]);
        }
    }

    private static Map<String, List<SubEntry>> deepCopySubs(
            Map<String, LinkedHashMap<String, Qos>> subs) {
        Map<String, List<SubEntry>> out = new TreeMap<>();
        subs.forEach((c, m) -> {
            if (!m.isEmpty()) {
                List<SubEntry> list = new ArrayList<>();
                m.forEach((filter, qos) -> list.add(new SubEntry(filter, qos)));
                out.put(c, List.copyOf(list));
            }
        });
        return Map.copyOf(out);
    }

    private static Map<String, List<MsgEntry>> deepCopyQueued(
            Map<String, LinkedHashMap<Long, MsgEntry>> queued,
            Map<String, Long> maxPacketId) {
        Map<String, List<MsgEntry>> out = new TreeMap<>();
        queued.forEach((c, m) -> {
            if (!m.isEmpty()) {
                out.put(c, List.copyOf(m.values()));
            }
        });
        return Map.copyOf(out);
    }

    private static Map<String, Long> nextIds(Map<String, Long> maxPacketId) {
        Map<String, Long> out = new TreeMap<>();
        maxPacketId.forEach((c, max) -> out.put(c, max + 1));
        return Map.copyOf(out);
    }

    /**
     * 用当前状态重写 WAL：先写临时文件再原子替换。
     */
    private void compact(Snapshot snapshot) {
        synchronized (writeLock) {
            Path tmp = walFile.resolveSibling("gateway.wal.compact");
            try (var writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                for (String c : snapshot.sessions()) {
                    writer.write("SESSION\t" + enc(c));
                    writer.newLine();
                    for (SubEntry s : snapshot.subscriptions().getOrDefault(c, List.of())) {
                        writer.write("SUB\t" + enc(c) + "\t" + enc(s.filter())
                                + "\t" + s.qos().ordinal());
                        writer.newLine();
                    }
                    for (MsgEntry m : snapshot.queued().getOrDefault(c, List.of())) {
                        writer.write("QUEUE\t" + enc(c) + "\t" + m.packetId() + "\t"
                                + m.qos().ordinal() + "\t" + enc(m.topic()) + "\t"
                                + Base64.getEncoder().encodeToString(m.payload()));
                        writer.newLine();
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("压缩 WAL 失败", e);
            }
            try {
                Files.move(tmp, walFile, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("替换 WAL 失败", e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            closed = true;
        }
    }

    private static String enc(String s) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String dec(String s) {
        return new String(Base64.getUrlDecoder().decode(s), StandardCharsets.UTF_8);
    }
}
