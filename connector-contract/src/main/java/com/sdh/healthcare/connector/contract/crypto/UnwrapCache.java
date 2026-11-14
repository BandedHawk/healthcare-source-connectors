/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import java.security.PublicKey;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A key provider that remembers the data keys it has unwrapped, so that
 * the items a landing node wrote under one data key (a group, since
 * docs/PERFORMANCE.md §3.1) cost one KMS call to read, not one each
 * (§10). A small LRU, by KEK and wrapped key; a key dropped from it is
 * wiped. Every caller gets a copy of its own, to wipe as it would any
 * unwrapped key. Wrapping and signing go straight to the provider.
 * <p>
 * The keys are in the heap while kept, as each is while used: heap dumps
 * stay off (docs/PERFORMANCE.md §3.5).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class UnwrapCache implements KeyProvider
{

    /** Unwraps what isn't kept. */
    private final KeyProvider provider;

    /** The keys, by KEK and wrapped key, least recently used first. */
    private final Map<String, byte[]> kept;

    /**
     * Creates a cache.
     *
     * @param provider unwraps the keys
     * @param size     how many to keep; at least 1
     * @throws IllegalArgumentException if the size is under 1
     */
    public UnwrapCache(KeyProvider provider, int size)
    {
        if (size < 1)
        {
            throw new IllegalArgumentException("size must be at least 1");
        }
        this.provider = provider;
        this.kept = new LinkedHashMap<>(16, 0.75f, true)
        {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> old)
            {
                if (size() > size)
                {
                    Arrays.fill(old.getValue(), (byte) 0);
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    public WrappedKey wrap(String keyRef, byte[] dataKey)
    {
        return provider.wrap(keyRef, dataKey);
    }

    /**
     * {@inheritDoc}
     * <p>
     * From the cache if it was unwrapped before; the provider is called
     * outside the lock, so a slow KMS holds up no other key.
     */
    @Override
    public byte[] unwrap(WrappedKey wrapped)
    {
        String id = wrapped.keyRef() + ":"
                + Base64.getEncoder().encodeToString(wrapped.wrapped());
        synchronized (kept)
        {
            byte[] key = kept.get(id);
            if (key != null)
            {
                return key.clone();
            }
        }
        byte[] key = provider.unwrap(wrapped);
        synchronized (kept)
        {
            kept.putIfAbsent(id, key.clone());
        }
        return key;
    }

    @Override
    public byte[] sign(String keyRef, byte[] digest)
    {
        return provider.sign(keyRef, digest);
    }

    @Override
    public PublicKey publicKey(String keyRef)
    {
        return provider.publicKey(keyRef);
    }

    /** Wipes and forgets every key kept. */
    public void clear()
    {
        synchronized (kept)
        {
            kept.values().forEach(key -> Arrays.fill(key, (byte) 0));
            kept.clear();
        }
    }
}
