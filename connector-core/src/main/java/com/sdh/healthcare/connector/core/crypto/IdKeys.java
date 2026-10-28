package com.sdh.healthcare.connector.core.crypto;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import java.security.SecureRandom;
import java.text.ParseException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.Table;

/**
 * The ID key (design §7.1): a 256-bit HMAC secret generated at first start and stored, wrapped by a KEK,
 * in the table property {@value #PROPERTY}. Never rotated, because section IDs and downstream dedup depend
 * on it.
 */
public final class IdKeys {
    public static final String PROPERTY = "sdh.id-key";

    private IdKeys() {}

    /** Unwraps the stored ID key, creating and storing one first if the table has none. */
    public static byte[] loadOrCreate(Table table, FencedCommitter committer, KeyProvider keys, String kekRef) {
        table.refresh();
        String stored = table.properties().get(PROPERTY);
        if (stored == null) {
            byte[] key = new byte[32];
            new SecureRandom().nextBytes(key);
            String encoded = encode(keys.wrap(kekRef, key));
            committer.commit(table, tx -> tx.updateProperties().set(PROPERTY, encoded).commit());
            table.refresh();
            stored = table.properties().get(PROPERTY);
        }
        byte[] key = keys.unwrap(decode(stored));
        if (key.length != 32) {
            throw new IllegalStateException("Stored ID key has the wrong length");
        }
        return key;
    }

    static String encode(WrappedKey w) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kekRef", w.keyRef());
        m.put("wrapped", Base64.getEncoder().encodeToString(w.wrapped()));
        return JSONObjectUtils.toJSONString(m);
    }

    static WrappedKey decode(String s) {
        try {
            Map<String, Object> o = JSONObjectUtils.parse(s);
            return new WrappedKey(JSONObjectUtils.getString(o, "kekRef"),
                    Base64.getDecoder().decode(JSONObjectUtils.getString(o, "wrapped")));
        } catch (ParseException | RuntimeException e) {
            throw new IllegalStateException("Stored ID key is unreadable");
        }
    }
}
