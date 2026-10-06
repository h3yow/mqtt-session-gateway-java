package com.github.h3yow.mqttsessiongatewayjava.gateway;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于本地目录的快照存储：单文件 JSON，临时文件 + 原子 rename，写入后 fsync。
 * 不依赖任何外部服务。
 *
 * <p>每次保存都是全量快照；rename 原子生效，进程在任意时刻崩溃后，
 * 磁盘上要么是旧快照、要么是新快照，不会出现半截文件。
 */
public class LocalFileStateStore implements GatewayStateStore {

    private static final Logger log = LoggerFactory.getLogger(LocalFileStateStore.class);

    private static final String STATE_FILE = "gateway-state.json";
    private static final String TMP_FILE = "gateway-state.json.tmp";

    private final Path stateFile;
    private final Path tmpFile;
    private final ObjectMapper mapper;

    LocalFileStateStore(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new GatewayException(RejectReason.PERSISTENCE_IO_ERROR,
                    "cannot create data dir '" + dataDir + "': " + e.getMessage());
        }
        this.stateFile = dataDir.resolve(STATE_FILE);
        this.tmpFile = dataDir.resolve(TMP_FILE);
        this.mapper = new ObjectMapper();
        log.info("[persistence] 本地状态存储已就绪 dataDir={} stateFile={}", dataDir, stateFile);
    }

    @Override
    public Optional<GatewaySnapshot> load() {
        if (!Files.exists(stateFile)) {
            log.info("[persistence] 未发现历史快照 stateFile={}（首次启动）", stateFile);
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(stateFile);
            GatewaySnapshot snapshot = mapper.readValue(bytes, GatewaySnapshot.class);
            log.info("[persistence] 快照加载成功 sessions={} lastMessageId={}",
                    snapshot.sessions().size(), snapshot.lastMessageId());
            return Optional.of(snapshot);
        } catch (IOException e) {
            throw new GatewayException(RejectReason.PERSISTENCE_IO_ERROR,
                    "cannot read state file '" + stateFile + "': " + e.getMessage());
        }
    }

    @Override
    public void save(GatewaySnapshot snapshot) {
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(snapshot);
            // 写临时文件并 fsync，再原子替换正式文件，保证崩溃后不出现半截快照
            try (FileChannel ch = FileChannel.open(tmpFile,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(ByteBuffer.wrap(bytes));
                ch.force(true);
            }
            try {
                Files.move(tmpFile, stateFile,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmpFile, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new GatewayException(RejectReason.PERSISTENCE_IO_ERROR,
                    "cannot write state file '" + stateFile + "': " + e.getMessage());
        }
    }

    /** 仅供诊断：快照文件路径。 */
    Path stateFile() {
        return stateFile;
    }
}
