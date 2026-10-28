package com.sdh.healthcare.connector.core.storage;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.crypto.MacedJson;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import java.text.ParseException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Archive progress (design §5.1, §9.1), in table property {@code sdh.archive.<connectorId>} of the archive table and
 * committed with each archive batch. A restart needs only the next block and the chain value so far.
 *
 * @param chainHash base64 of H after the last archived block (32 zero bytes before the first)
 * @param digest    the finished file digest; set once {@code done}
 */
public record ArchiveCheckpoint(UUID fileId, long nextBlock, String chainHash, boolean done, String digest,
        Instant finishedAt) {

    public static final String PROPERTY_PREFIX = "sdh.archive.";

    public static String propertyKey(String connectorId) {
        return PROPERTY_PREFIX + connectorId;
    }

    public static MacedJson<ArchiveCheckpoint> codec(SectionIds ids) {
        return new MacedJson<>(ids, "sdh-archive", ArchiveCheckpoint::canonical, ArchiveCheckpoint::parse);
    }

    private static Map<String, Object> canonical(ArchiveCheckpoint c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fileId", c.fileId().toString());
        m.put("nextBlock", c.nextBlock());
        m.put("chainHash", c.chainHash());
        m.put("done", c.done());
        if (c.done()) {
            m.put("digest", c.digest());
            m.put("finishedAt", c.finishedAt().toString());
        }
        return m;
    }

    private static ArchiveCheckpoint parse(Map<String, Object> o) {
        try {
            boolean done = JSONObjectUtils.getBoolean(o, "done");
            return new ArchiveCheckpoint(UUID.fromString(JSONObjectUtils.getString(o, "fileId")),
                    JSONObjectUtils.getLong(o, "nextBlock"), JSONObjectUtils.getString(o, "chainHash"), done,
                    done ? JSONObjectUtils.getString(o, "digest") : null,
                    done ? Instant.parse(JSONObjectUtils.getString(o, "finishedAt")) : null);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
