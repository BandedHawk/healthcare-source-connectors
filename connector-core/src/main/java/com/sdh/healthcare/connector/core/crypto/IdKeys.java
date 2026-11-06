/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
 * Loads the ID key (design §7.1): a 256-bit HMAC secret, made when the
 * connector first starts and stored, wrapped by a key-encryption key (KEK),
 * in the table property {@value #PROPERTY}. It is never rotated, because
 * record IDs, and so downstream deduplication, depend on it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class IdKeys
{

    /** The table property the wrapped ID key is stored in. */
    public static final String PROPERTY = "sdh.id-key";

    /** How long the ID key is: 32 bytes, 256 bits. */
    private static final int KEY_BYTES = 32;

    /** The stored JSON's field naming the KEK that wraps the key. */
    private static final String KEK_REF_KEY = "kekRef";

    /** The stored JSON's field holding the wrapped key, in base64. */
    private static final String WRAPPED_KEY = "wrapped";

    /** Not used: there are only static methods. */
    private IdKeys()
    {
    }

    /**
     * Unwraps the stored ID key, first making and storing one if the table
     * has none.
     *
     * @param table     the table that holds the key
     * @param committer commits the new key, if one is made, under the lease
     * @param keys      wraps and unwraps the key
     * @param kekRef    the KEK to wrap a new key with
     * @return the ID key
     * @throws IllegalStateException if the stored key can't be read or has
     *                               the wrong length
     */
    public static byte[] loadOrCreate(Table table, FencedCommitter committer,
            KeyProvider keys, String kekRef)
    {
        table.refresh();
        String stored = table.properties().get(PROPERTY);
        if (stored == null)
        {
            byte[] newKey = new byte[KEY_BYTES];
            new SecureRandom().nextBytes(newKey);
            String encoded = encode(keys.wrap(kekRef, newKey));
            committer.commit(table, transaction -> transaction
                    .updateProperties().set(PROPERTY, encoded).commit());
            // Read back the key as committed.
            table.refresh();
            stored = table.properties().get(PROPERTY);
        }
        return unwrap(stored, keys);
    }

    /**
     * Unwraps the stored ID key, without ever making one: for read-only
     * tools (design §11.4), which must not write to the tables.
     *
     * @param table the table that holds the key
     * @param keys  unwraps the key
     * @return the ID key
     * @throws IllegalStateException if there is no stored key yet, or it
     *                               can't be read or has the wrong length
     */
    public static byte[] load(Table table, KeyProvider keys)
    {
        table.refresh();
        String stored = table.properties().get(PROPERTY);
        if (stored == null)
        {
            throw new IllegalStateException("No ID key stored yet: the"
                    + " connector has never started on these tables");
        }
        return unwrap(stored, keys);
    }

    /**
     * Unwraps a stored ID key and checks its length.
     *
     * @param stored the table property's value
     * @param keys   unwraps the key
     * @return the ID key
     * @throws IllegalStateException if the key can't be read or has the
     *                               wrong length
     */
    private static byte[] unwrap(String stored, KeyProvider keys)
    {
        byte[] key = keys.unwrap(decode(stored));
        if (key.length != KEY_BYTES)
        {
            throw new IllegalStateException(
                    "Stored ID key has the wrong length");
        }
        return key;
    }

    /**
     * Writes a wrapped key as the table property's JSON.
     *
     * @param wrappedKey the wrapped key
     * @return the JSON, naming the KEK and holding the key in base64
     */
    static String encode(WrappedKey wrappedKey)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put(KEK_REF_KEY, wrappedKey.keyRef());
        fields.put(WRAPPED_KEY,
                Base64.getEncoder().encodeToString(wrappedKey.wrapped()));
        return JSONObjectUtils.toJSONString(fields);
    }

    /**
     * Reads a wrapped key from the table property's JSON.
     *
     * @param json the JSON, as {@link #encode} writes it
     * @return the wrapped key
     * @throws IllegalStateException if the JSON can't be read
     */
    static WrappedKey decode(String json)
    {
        try
        {
            Map<String, Object> fields = JSONObjectUtils.parse(json);
            return new WrappedKey(
                    JSONObjectUtils.getString(fields, KEK_REF_KEY),
                    Base64.getDecoder().decode(
                            JSONObjectUtils.getString(fields, WRAPPED_KEY)));
        }
        catch (ParseException | RuntimeException unreadable)
        {
            throw new IllegalStateException("Stored ID key is unreadable");
        }
    }
}
