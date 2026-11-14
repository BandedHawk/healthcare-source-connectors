/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UnwrapCache}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class UnwrapCacheTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    UnwrapCacheTest()
    {
    }

    /** Unwraps by giving the wrapped bytes back, counting the calls. */
    static final class Counting implements KeyProvider
    {

        /** The keys asked for, in order. */
        final List<WrappedKey> asked = new ArrayList<>();

        /** Creates the provider. */
        Counting()
        {
        }

        @Override
        public WrappedKey wrap(String keyRef, byte[] dataKey)
        {
            return new WrappedKey(keyRef, dataKey.clone());
        }

        @Override
        public byte[] unwrap(WrappedKey wrapped)
        {
            asked.add(wrapped);
            return wrapped.wrapped().clone();
        }
    }

    /**
     * A key unwrapped again comes from the cache, as a copy the caller may
     * wipe without harming the next.
     */
    @Test
    void unwrapsEachKeyOnceAndHandsOutCopies()
    {
        Counting provider = new Counting();
        UnwrapCache cache = new UnwrapCache(provider, 4);
        WrappedKey wrapped = new WrappedKey("kek", new byte[] {1, 2, 3});
        byte[] first = cache.unwrap(wrapped);
        first[0] = 0;
        byte[] second = cache.unwrap(new WrappedKey("kek",
                new byte[] {1, 2, 3}));
        assertThat(second).containsExactly(1, 2, 3);
        assertThat(provider.asked).hasSize(1);

        cache.unwrap(new WrappedKey("other-kek", new byte[] {1, 2, 3}));
        assertThat(provider.asked).hasSize(2);
    }

    /**
     * Beyond its size, the least recently used key is dropped and asked
     * for again; clearing drops them all.
     */
    @Test
    void dropsTheLeastRecentlyUsed()
    {
        Counting provider = new Counting();
        UnwrapCache cache = new UnwrapCache(provider, 2);
        WrappedKey a = new WrappedKey("kek", new byte[] {1});
        WrappedKey b = new WrappedKey("kek", new byte[] {2});
        WrappedKey c = new WrappedKey("kek", new byte[] {3});
        cache.unwrap(a);
        cache.unwrap(b);
        cache.unwrap(a);
        cache.unwrap(c);
        cache.unwrap(a);
        assertThat(provider.asked).hasSize(3);
        cache.unwrap(b);
        assertThat(provider.asked).hasSize(4);
        cache.clear();
        cache.unwrap(a);
        assertThat(provider.asked).hasSize(5);
    }
}
