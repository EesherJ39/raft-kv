package com.example.raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bounded binary codec for internal RPCs; deliberately dependency-free. */
public final class RpcCodec {
    private static final int VERSION = 1;
    private static final int MAX_STRING_BYTES = 4 * 1024 * 1024;
    private static final int MAX_COLLECTION_SIZE = 1_000_000;

    private RpcCodec() {}

    public static byte[] encodeRequestVote(RequestVote.Request request) throws IOException {
        return encode(out -> {
            out.writeInt(request.term());
            writeString(out, request.candidateId());
            out.writeLong(request.lastLogIndex());
            out.writeInt(request.lastLogTerm());
        });
    }

    public static RequestVote.Request decodeRequestVote(byte[] bytes) throws IOException {
        return decode(bytes, in -> new RequestVote.Request(
                in.readInt(), readString(in), in.readLong(), in.readInt()));
    }

    public static byte[] encodeRequestVoteResponse(RequestVote.Response response) throws IOException {
        return encode(out -> {
            out.writeInt(response.term());
            out.writeBoolean(response.voteGranted());
        });
    }

    public static RequestVote.Response decodeRequestVoteResponse(byte[] bytes) throws IOException {
        return decode(bytes, in -> new RequestVote.Response(in.readInt(), in.readBoolean()));
    }

    public static byte[] encodeAppendEntries(AppendEntries.Request request) throws IOException {
        return encode(out -> {
            out.writeInt(request.term());
            writeString(out, request.leaderId());
            out.writeLong(request.prevLogIndex());
            out.writeInt(request.prevLogTerm());
            out.writeLong(request.leaderCommit());
            out.writeInt(request.entries().size());
            for (LogEntry entry : request.entries()) writeLogEntry(out, entry);
        });
    }

    public static AppendEntries.Request decodeAppendEntries(byte[] bytes) throws IOException {
        return decode(bytes, in -> {
            int term = in.readInt();
            String leader = readString(in);
            long previousIndex = in.readLong();
            int previousTerm = in.readInt();
            long leaderCommit = in.readLong();
            int count = readCount(in, "log entries");
            List<LogEntry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) entries.add(readLogEntry(in));
            return new AppendEntries.Request(term, leader, previousIndex, previousTerm, entries, leaderCommit);
        });
    }

    public static byte[] encodeAppendEntriesResponse(AppendEntries.Response response) throws IOException {
        return encode(out -> {
            out.writeInt(response.term());
            out.writeBoolean(response.success());
            out.writeLong(response.matchIndex());
            out.writeLong(response.conflictIndex());
            out.writeInt(response.conflictTerm());
        });
    }

    public static AppendEntries.Response decodeAppendEntriesResponse(byte[] bytes) throws IOException {
        return decode(bytes, in -> new AppendEntries.Response(
                in.readInt(), in.readBoolean(), in.readLong(), in.readLong(), in.readInt()));
    }

    public static byte[] encodeInstallSnapshot(InstallSnapshot.Request request) throws IOException {
        return encode(out -> {
            out.writeInt(request.term());
            writeString(out, request.leaderId());
            out.writeLong(request.lastIncludedIndex());
            out.writeInt(request.lastIncludedTerm());
            out.writeLong(request.leaderCommit());
            out.writeInt(request.values().size());
            for (Map.Entry<String, String> entry : request.values().entrySet()) {
                writeString(out, entry.getKey());
                writeString(out, entry.getValue());
            }
        });
    }

    public static InstallSnapshot.Request decodeInstallSnapshot(byte[] bytes) throws IOException {
        return decode(bytes, in -> {
            int term = in.readInt();
            String leader = readString(in);
            long lastIncludedIndex = in.readLong();
            int lastIncludedTerm = in.readInt();
            long leaderCommit = in.readLong();
            int count = readCount(in, "snapshot entries");
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) values.put(readString(in), readString(in));
            return new InstallSnapshot.Request(
                    term, leader, lastIncludedIndex, lastIncludedTerm, values, leaderCommit);
        });
    }

    public static byte[] encodeInstallSnapshotResponse(InstallSnapshot.Response response) throws IOException {
        return encode(out -> {
            out.writeInt(response.term());
            out.writeBoolean(response.accepted());
            out.writeLong(response.matchIndex());
        });
    }

    public static InstallSnapshot.Response decodeInstallSnapshotResponse(byte[] bytes) throws IOException {
        return decode(bytes, in -> new InstallSnapshot.Response(in.readInt(), in.readBoolean(), in.readLong()));
    }

    private static void writeLogEntry(DataOutputStream out, LogEntry entry) throws IOException {
        out.writeLong(entry.index());
        out.writeInt(entry.term());
        out.writeByte(entry.command().ordinal());
        writeString(out, entry.key());
        writeString(out, entry.value());
    }

    private static LogEntry readLogEntry(DataInputStream in) throws IOException {
        long index = in.readLong();
        int term = in.readInt();
        int ordinal = in.readUnsignedByte();
        if (ordinal >= LogEntry.Command.values().length) throw new IOException("unknown command type");
        return new LogEntry(index, term, LogEntry.Command.values()[ordinal], readString(in), readString(in));
    }

    private static byte[] encode(Encoder encoder) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(VERSION);
            encoder.write(out);
        }
        return bytes.toByteArray();
    }

    private static <T> T decode(byte[] bytes, Decoder<T> decoder) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != VERSION) throw new IOException("unsupported RPC version");
            T value = decoder.read(in);
            if (in.available() != 0) throw new IOException("trailing RPC bytes");
            return value;
        } catch (EOFException error) {
            throw new IOException("truncated RPC payload", error);
        }
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

    private static int readCount(DataInputStream in, String label) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > MAX_COLLECTION_SIZE) throw new IOException("invalid " + label + " count");
        return count;
    }

    @FunctionalInterface
    private interface Encoder { void write(DataOutputStream out) throws IOException; }
    @FunctionalInterface
    private interface Decoder<T> { T read(DataInputStream in) throws IOException; }
}
