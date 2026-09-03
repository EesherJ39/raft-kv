package com.example.raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * Versioned, checksummed Raft state persisted with fsync plus atomic replacement.
 * The deliberately simple copy-on-write format favors auditable crash semantics.
 */
public final class FileRaftStorage implements RaftStorage {
    private static final int MAGIC = 0x524B5653; // RKVS
    private static final int VERSION = 1;
    private static final int MAX_FILE_BYTES = 128 * 1024 * 1024;
    private static final int MAX_STRING_BYTES = 4 * 1024 * 1024;
    private static final int MAX_LOG_ENTRIES = 2_000_000;
    private final Path path;

    public FileRaftStorage(Path path) {
        this.path = path.toAbsolutePath().normalize();
    }

    @Override
    public synchronized StoredState load() throws IOException {
        if (!Files.exists(path)) return StoredState.empty();
        long size = Files.size(path);
        if (size < 16 || size > MAX_FILE_BYTES) {
            throw new IOException("invalid Raft state size: " + size);
        }

        byte[] file = Files.readAllBytes(path);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(file))) {
            if (in.readInt() != MAGIC) throw new IOException("invalid Raft state magic");
            if (in.readInt() != VERSION) throw new IOException("unsupported Raft state version");
            int payloadLength = in.readInt();
            if (payloadLength < 0 || payloadLength > file.length - 16) {
                throw new IOException("invalid Raft state payload length");
            }
            byte[] payload = in.readNBytes(payloadLength);
            if (payload.length != payloadLength) throw new EOFException("truncated Raft state");
            int expectedCrc = in.readInt();
            CRC32 crc = new CRC32();
            crc.update(payload);
            if ((int) crc.getValue() != expectedCrc) throw new IOException("Raft state checksum mismatch");
            if (in.available() != 0) throw new IOException("trailing bytes in Raft state");
            return decodePayload(payload);
        }
    }

    @Override
    public synchronized void save(StoredState state) throws IOException {
        validate(state);
        byte[] payload = encodePayload(state);
        CRC32 crc = new CRC32();
        crc.update(payload);

        ByteBuffer file = ByteBuffer.allocate(16 + payload.length);
        file.putInt(MAGIC).putInt(VERSION).putInt(payload.length).put(payload).putInt((int) crc.getValue());
        file.flip();

        Path parent = path.getParent();
        if (parent != null) Files.createDirectories(parent);
        Path temp = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(
                temp,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            while (file.hasRemaining()) channel.write(file);
            channel.force(true);
        }
        try {
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static byte[] encodePayload(StoredState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(state.currentTerm());
            writeNullableString(out, state.votedFor());
            out.writeLong(state.commitIndex());
            out.writeLong(state.snapshot().lastIncludedIndex());
            out.writeInt(state.snapshot().lastIncludedTerm());
            Map<String, String> orderedSnapshot = new TreeMap<>(state.snapshot().values());
            out.writeInt(orderedSnapshot.size());
            for (Map.Entry<String, String> entry : orderedSnapshot.entrySet()) {
                writeString(out, entry.getKey());
                writeString(out, entry.getValue());
            }
            out.writeInt(state.log().size());
            for (LogEntry entry : state.log()) {
                out.writeLong(entry.index());
                out.writeInt(entry.term());
                out.writeByte(entry.command().ordinal());
                writeString(out, entry.key());
                writeString(out, entry.value());
            }
        }
        return bytes.toByteArray();
    }

    private static StoredState decodePayload(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int term = in.readInt();
            String votedFor = readNullableString(in);
            long commitIndex = in.readLong();
            long snapshotIndex = in.readLong();
            int snapshotTerm = in.readInt();

            int snapshotSize = checkedCount(in.readInt(), "snapshot entries");
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < snapshotSize; i++) {
                values.put(readString(in), readString(in));
            }

            int logSize = checkedCount(in.readInt(), "log entries");
            if (logSize > MAX_LOG_ENTRIES) throw new IOException("too many log entries");
            List<LogEntry> log = new ArrayList<>(logSize);
            for (int i = 0; i < logSize; i++) {
                long index = in.readLong();
                int entryTerm = in.readInt();
                int commandOrdinal = in.readUnsignedByte();
                if (commandOrdinal >= LogEntry.Command.values().length) {
                    throw new IOException("unknown command type");
                }
                log.add(new LogEntry(
                        index,
                        entryTerm,
                        LogEntry.Command.values()[commandOrdinal],
                        readString(in),
                        readString(in)));
            }
            if (in.available() != 0) throw new IOException("trailing payload bytes");
            StoredState state = new StoredState(
                    term,
                    votedFor,
                    commitIndex,
                    new Snapshot(snapshotIndex, snapshotTerm, values),
                    log);
            validate(state);
            return state;
        } catch (EOFException e) {
            throw new IOException("truncated Raft state payload", e);
        }
    }

    private static void validate(StoredState state) throws IOException {
        Snapshot snapshot = state.snapshot();
        long expected = snapshot.lastIncludedIndex() + 1;
        for (LogEntry entry : state.log()) {
            if (entry.index() != expected++) throw new IOException("non-contiguous Raft log");
        }
        long lastIndex = state.log().isEmpty()
                ? snapshot.lastIncludedIndex()
                : state.log().get(state.log().size() - 1).index();
        if (state.commitIndex() < snapshot.lastIncludedIndex() || state.commitIndex() > lastIndex) {
            throw new IOException("commit index outside persisted log");
        }
    }

    private static int checkedCount(int count, String label) throws IOException {
        if (count < 0 || count > MAX_LOG_ENTRIES) throw new IOException("invalid " + label + ": " + count);
        return count;
    }

    private static void writeNullableString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) writeString(out, value);
    }

    private static String readNullableString(DataInputStream in) throws IOException {
        return in.readBoolean() ? readString(in) : null;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > MAX_STRING_BYTES) throw new IOException("string too large");
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) throw new IOException("invalid string length");
        byte[] encoded = in.readNBytes(length);
        if (encoded.length != length) throw new EOFException("truncated string");
        return new String(encoded, StandardCharsets.UTF_8);
    }
}
