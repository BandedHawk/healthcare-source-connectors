/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Small LRU of unwrapped batch data keys, so a batch's sections cost one
 * KMS call, not one each.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class BatchKeyCache
{

    /** Unwraps batch data keys. */
    private final KeyProvider provider;

    /** The unwrapped keys, by batch, least recently used first. */
    private final Map<UUID, byte[]> cache;

    /**
     * Creates a cache.
     *
     * @param provider unwraps batch data keys
     * @param size how many keys to keep; the least recently used is wiped
     *        and dropped beyond that
     */
    BatchKeyCache(KeyProvider provider, int size)
    {
        this.provider = provider;
        this.cache = new LinkedHashMap<>(16, 0.75f, true)
        {
            @Override
            protected boolean removeEldestEntry(Map.Entry<UUID, byte[]> eldest)
            {
                if (size() > size)
                {
                    Arrays.fill(eldest.getValue(), (byte) 0);
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * A batch's data key, unwrapped on first use.
     *
     * @param batchId the batch
     * @param wrapped the batch's wrapped data key
     * @return a copy of the key, for the caller to wipe
     */
    synchronized byte[] get(UUID batchId, WrappedKey wrapped)
    {
        byte[] key = cache.get(batchId);
        if (key == null)
        {
            key = provider.unwrap(wrapped);
            cache.put(batchId, key);
        }
        return key.clone();
    }

    /**
     * Wipes and forgets every key.
     */
    synchronized void clear()
    {
        cache.values().forEach(key -> Arrays.fill(key, (byte) 0));
        cache.clear();
    }
}
