package com.github.h3yow.mqttsessiongatewayjava.gateway.persist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 本地文件存储：单文件 JSONL 追加日志 {@code journal.jsonl}。
 *
 * <p>每行要么是一条 {@link StoredState.Change}（含 {@code type} 字段），
 * 要么是一条全量快照行（含 {@code s:snapshot} 字段）。快照行之后的变更行才生效，
 * 重放时遇到快照行即以其为新基线，因此压缩（写临时文件后原子替换）天然幂等安全。
 *
 * <p>一批变更序列化为多行后一次写入并 {@code force(true)}，保证网关进程停止后
 * 已确认的状态已在磁盘上，不依赖任何外部服务。
 */
public class LocalFileGatewayStore implements GatewayStore {

    private static final Logger log = LoggerFactory.getLogger(LocalFileGatewayStore.class);

    private static final String JOURNAL_FILE = "journal.jsonl";
    private static final String TMP_FILE = "journal.jsonl.tmp";

    /** 快照行包装：仅用于与变更行区分。 */
    private record SnapshotLine(StoredState.Snapshot snapshot) {
    }

    private final Path journal;
    private final Path tmpJournal;
    private final ObjectMapper mapper = new ObjectMapper();
    private FileChannel appender;

    public LocalFileGatewayStore(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create data dir: " + dataDir, e);
        }
        this.journal = dataDir.resolve(JOURNAL_FILE);
        this.tmpJournal = dataDir.resolve(TMP_FILE);
    }

    @Override
    public synchronized StoredState.Snapshot replay() {
        if (!Files.exists(journal)) {
            return new StoredState.Snapshot(Map.of(), 0L);
        }
        MutableState state = new MutableState();
        long lineNo = 0;
        try (BufferedReader reader = Files.newBufferedReader(journal)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node;
                try {
                    node = mapper.readTree(line);
                } catch (IOException parseError) {
                    // 追加模式下只有崩溃残留的最后一行可能残缺：停止读取，之前的完整行仍然有效
                    log.warn("[replay] journal 第 {} 行解析失败（视为崩溃残留的残缺尾行，停止重放后续内容）: {}",
                            lineNo, parseError.getMessage());
                    break;
                }
                if (node.has("snapshot")) {
                    StoredState.Snapshot snap = mapper.treeToValue(node, SnapshotLine.class).snapshot();
                    state.reset(snap);
                } else {
                    state.apply(mapper.treeToValue(node, StoredState.Change.class));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read journal: " + journal, e);
        }
        StoredState.Snapshot result = state.toSnapshot();
        log.info("[replay] dataDir={} sessions={} maxMessageId={}",
                journal.getParent(), result.sessions().size(), result.maxMessageId());
        return result;
    }

    @Override
    public synchronized void appendBatch(List<StoredState.Change> changes) {
        if (changes.isEmpty()) {
            return;
        }
        try {
            ensureAppender();
            StringBuilder sb = new StringBuilder();
            for (StoredState.Change change : changes) {
                sb.append(mapper.writeValueAsString(change)).append('\n');
            }
            byte[] bytes = sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                appender.write(buffer);
            }
            appender.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot append to journal: " + journal, e);
        }
    }

    @Override
    public synchronized void compact(StoredState.Snapshot snapshot) {
        try {
            // 先写临时文件并落盘，再原子替换：崩溃时要么还是旧日志，要么是新快照，不会出现中间态
            try (FileChannel ch = FileChannel.open(tmpJournal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                byte[] bytes = (mapper.writeValueAsString(new SnapshotLine(snapshot)) + '\n')
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    ch.write(buffer);
                }
                ch.force(true);
            }
            try {
                Files.move(tmpJournal, journal, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicNotSupported) {
                Files.move(tmpJournal, journal, StandardCopyOption.REPLACE_EXISTING);
            }
            // 替换后重新打开追加句柄，避免旧句柄仍指向被替换的 inode
            closeAppender();
            log.debug("[compact] journal 已压缩为快照: sessions={} maxMessageId={}",
                    snapshot.sessions().size(), snapshot.maxMessageId());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot compact journal in: " + journal.getParent(), e);
        }
    }

    @Override
    public synchronized void close() {
        closeAppender();
    }

    private void ensureAppender() throws IOException {
        if (appender == null || !appender.isOpen()) {
            appender = FileChannel.open(journal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }
    }

    private void closeAppender() {
        if (appender != null) {
            try {
                appender.close();
            } catch (IOException ignored) {
                // 关闭时忽略错误
            }
            appender = null;
        }
    }

    /** 重放累加器：把快照与之后的变更合并为最终快照。 */
    private static final class MutableState {

        private static final class MutableSession {
            final Map<String, StoredState.StoredSubscription> subscriptions = new LinkedHashMap<>();
            final List<StoredState.StoredBacklogEntry> backlog = new ArrayList<>();
            final Map<Long, StoredState.StoredMessage> pending = new LinkedHashMap<>();
            final Set<Long> ackedIds = new LinkedHashSet<>();
        }

        private final Map<String, MutableSession> sessions = new LinkedHashMap<>();
        private long maxMessageId;

        void reset(StoredState.Snapshot snap) {
            sessions.clear();
            maxMessageId = snap.maxMessageId();
            if (snap.sessions() != null) {
                for (StoredState.StoredSession s : snap.sessions().values()) {
                    sessions.put(s.clientId(), toMutable(s));
                }
            }
        }

        void apply(StoredState.Change c) {
            trackId(c.ackedId());
            trackId(c.messageId());
            switch (c.type()) {
                case "PERSIST_SESSION" -> sessions.computeIfAbsent(c.clientId(), k -> new MutableSession());
                case "ADD_SUBSCRIPTION" -> {
                    MutableSession s = sessions.computeIfAbsent(c.clientId(), k -> new MutableSession());
                    StoredState.StoredSubscription sub = c.subscription();
                    s.subscriptions.put(sub.filter(), sub); // 同过滤器重复订阅视为更新
                }
                case "ADD_BACKLOG" -> {
                    MutableSession s = sessions.computeIfAbsent(c.clientId(), k -> new MutableSession());
                    s.backlog.add(c.backlogEntry());
                    trackMessage(c.backlogEntry().message());
                }
                case "ADD_PENDING" -> {
                    MutableSession s = sessions.computeIfAbsent(c.clientId(), k -> new MutableSession());
                    s.pending.put(c.pendingMessage().id(), c.pendingMessage());
                    trackMessage(c.pendingMessage());
                }
                case "REMOVE_BACKLOG" -> {
                    MutableSession s = sessions.get(c.clientId());
                    long id = c.backlogMessage().id();
                    trackMessage(c.backlogMessage());
                    if (s != null) {
                        s.backlog.removeIf(e -> e.message().id() == id);
                    }
                }
                case "REMOVE_PENDING" -> {
                    MutableSession s = sessions.get(c.clientId());
                    if (s != null) {
                        s.pending.remove(c.messageId());
                    }
                }
                case "ADD_ACKED_ID" -> {
                    MutableSession s = sessions.computeIfAbsent(c.clientId(), k -> new MutableSession());
                    s.ackedIds.add(c.ackedId());
                }
                default -> log.warn("[replay] 未知变更类型，已跳过: {}", c.type());
            }
        }

        StoredState.Snapshot toSnapshot() {
            Map<String, StoredState.StoredSession> out = new LinkedHashMap<>();
            for (Map.Entry<String, MutableSession> e : sessions.entrySet()) {
                MutableSession s = e.getValue();
                out.put(e.getKey(), new StoredState.StoredSession(
                        e.getKey(),
                        List.copyOf(s.subscriptions.values()),
                        List.copyOf(s.backlog),
                        List.copyOf(s.pending.values()),
                        List.copyOf(s.ackedIds)));
            }
            return new StoredState.Snapshot(out, maxMessageId);
        }

        private MutableSession toMutable(StoredState.StoredSession s) {
            MutableSession m = new MutableSession();
            for (StoredState.StoredSubscription sub : s.subscriptions()) {
                m.subscriptions.put(sub.filter(), sub);
            }
            m.backlog.addAll(s.backlog());
            for (StoredState.StoredMessage msg : s.pendingAck()) {
                m.pending.put(msg.id(), msg);
                trackMessage(msg);
            }
            m.ackedIds.addAll(s.ackedIds());
            for (Long id : s.ackedIds()) {
                trackId(id);
            }
            for (StoredState.StoredBacklogEntry e : s.backlog()) {
                trackMessage(e.message());
            }
            return m;
        }

        private void trackMessage(StoredState.StoredMessage m) {
            if (m != null) {
                trackId(m.id());
            }
        }

        private void trackId(Long id) {
            if (id != null && id > maxMessageId) {
                maxMessageId = id;
            }
        }
    }
}
