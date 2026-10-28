package com.sdh.healthcare.connector.reader;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Small LRU of unwrapped batch data keys, so a batch's sections cost one KMS call, not one each. */
final class BatchKeyCache {
    private final KeyProvider provider;
    private final Map<UUID, byte[]> cache;

    BatchKeyCache(KeyProvider provider, int size) {
        this.provider = provider;
        this.cache = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<UUID, byte[]> e) {
                if (size() > size) {
                    Arrays.fill(e.getValue(), (byte) 0);
                    return true;
                }
                return false;
            }
        };
    }

    synchronized byte[] get(UUID batchId, WrappedKey wrapped) {
        byte[] k = cache.get(batchId);
        if (k == null) {
            k = provider.unwrap(wrapped);
            cache.put(batchId, k);
        }
        return k.clone();
    }

    synchronized void clear() {
        cache.values().forEach(v -> Arrays.fill(v, (byte) 0));
        cache.clear();
    }
}
