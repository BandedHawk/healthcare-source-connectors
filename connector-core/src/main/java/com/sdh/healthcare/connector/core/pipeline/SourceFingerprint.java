package com.sdh.healthcare.connector.core.pipeline;

import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import javax.crypto.Mac;

/**
 * Recognises the same file after a restart without reading all of it (design §5.1):
 * {@code size + ":" + base32(HMAC(idKey, "sdh-fingerprint" ‖ 0 ‖ first 1 MiB ‖ last 1 MiB))}. Keyed, so it
 * reveals nothing about the content.
 */
public final class SourceFingerprint {
    static final int EDGE = 1 << 20;

    private SourceFingerprint() {}

    public static String of(SeekableByteSource src, SectionIds ids) throws IOException {
        long size = src.size();
        Mac mac = ids.newLabelledMac("sdh-fingerprint");
        int head = (int) Math.min(EDGE, size);
        mac.update(src.readFully(0, head));
        int tail = (int) Math.min(EDGE, size);
        mac.update(src.readFully(size - tail, tail));
        return size + ":" + SectionIds.encode(mac.doFinal());
    }
}
