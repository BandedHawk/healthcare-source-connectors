package com.sdh.healthcare.connector.contract.message;

import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileCompleted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Converts messages to and from the JSON object carried inside the JWS. */
public final class PayloadCodec {

    private PayloadCodec() {}

    public static Map<String, Object> toJson(ConnectorMessage m) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("v", ContractVersion.CURRENT);
        o.put("type", m.type().wireName());
        o.put("messageId", m.messageId());
        o.put("fileId", m.fileId().toString());
        switch (m) {
            case FileStarted f -> {
                o.put("format", f.format());
                o.put("fileSize", f.fileSize());
                o.put("startedAt", f.startedAt().toString());
                o.put("connectorId", f.connectorId());
            }
            case SectionStored s -> {
                o.put("sectionId", s.sectionId());
                o.put("seq", s.seq());
                o.put("sourceOffset", s.sourceOffset());
                o.put("sourceLength", s.sourceLength());
                o.put("format", s.format());
                o.put("plaintextLength", s.plaintextLength());
                o.put("chunkCount", s.chunkCount());
                StorageRef r = s.storage();
                Map<String, Object> st = new LinkedHashMap<>();
                st.put("catalog", r.catalog());
                st.put("namespace", r.namespace());
                st.put("table", r.table());
                st.put("ingestedDate", r.ingestedDate().toString());
                st.put("snapshotId", r.snapshotId());
                st.put("kekRef", r.kekRef());
                o.put("storage", st);
            }
            case FileCompleted c -> {
                o.put("sectionCount", c.sectionCount());
                o.put("fileSize", c.fileSize());
                o.put("fileDigest", c.fileDigest());
                o.put("startedAt", c.startedAt().toString());
                o.put("completedAt", c.completedAt().toString());
            }
        }
        return o;
    }

    /** Parses a payload. Unknown fields are ignored so that minor contract versions stay compatible. */
    @SuppressWarnings("unchecked")
    public static ConnectorMessage fromJson(Map<String, Object> o) {
        String v = (String) o.get("v");
        if (!ContractVersion.isReadable(v)) {
            throw new UnsupportedContractVersionException(v);
        }
        MessageType type = MessageType.fromWire(str(o, "type"));
        UUID fileId = UUID.fromString(str(o, "fileId"));
        ConnectorMessage m =
                switch (type) {
                    case FILE_STARTED -> new FileStarted(
                            fileId,
                            str(o, "format"),
                            num(o, "fileSize"),
                            Instant.parse(str(o, "startedAt")),
                            str(o, "connectorId"));
                    case SECTION_STORED -> {
                        Map<String, Object> st = (Map<String, Object>) req(o, "storage");
                        yield new SectionStored(
                                str(o, "sectionId"),
                                fileId,
                                num(o, "seq"),
                                num(o, "sourceOffset"),
                                num(o, "sourceLength"),
                                str(o, "format"),
                                num(o, "plaintextLength"),
                                Math.toIntExact(num(o, "chunkCount")),
                                new StorageRef(
                                        str(st, "catalog"),
                                        str(st, "namespace"),
                                        str(st, "table"),
                                        LocalDate.parse(str(st, "ingestedDate")),
                                        num(st, "snapshotId"),
                                        str(st, "kekRef")));
                    }
                    case FILE_COMPLETED -> new FileCompleted(
                            fileId,
                            num(o, "sectionCount"),
                            num(o, "fileSize"),
                            str(o, "fileDigest"),
                            Instant.parse(str(o, "startedAt")),
                            Instant.parse(str(o, "completedAt")));
                };
        if (!m.messageId().equals(o.get("messageId"))) {
            throw new IllegalArgumentException("messageId does not match payload");
        }
        return m;
    }

    private static Object req(Map<String, Object> o, String k) {
        Object v = o.get(k);
        if (v == null) {
            throw new IllegalArgumentException("Missing field " + k);
        }
        return v;
    }

    private static String str(Map<String, Object> o, String k) {
        return (String) req(o, k);
    }

    private static long num(Map<String, Object> o, String k) {
        return ((Number) req(o, k)).longValue();
    }
}
