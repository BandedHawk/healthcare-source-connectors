/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Tests for {@link StoreVersioning}, against mocked S3 and GCS clients.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StoreVersioningTest
{

    /** A table's location in S3. */
    private static final String S3_TABLE =
            "s3://bronze/warehouse/sdh_ingest/landed_items";

    /** Creates the tests; JUnit runs each test on a new instance. */
    StoreVersioningTest()
    {
    }

    /** The bucket is a location's authority, and some locations have none. */
    @Test
    void theBucketIsTheLocationsAuthority()
    {
        assertThat(StoreVersioning.bucket(S3_TABLE)).contains("bronze");
        assertThat(StoreVersioning.bucket("gs://gold/wh/t")).contains("gold");
        assertThat(StoreVersioning.bucket(
                "abfss://silver@acct.dfs.core.windows.net/wh/t"))
                .contains("silver@acct.dfs.core.windows.net");
        assertThat(StoreVersioning.bucket("/var/data/wh/t")).isEmpty();
        assertThat(StoreVersioning.bucket("not a uri")).isEmpty();
    }

    /**
     * S3: enabled is on; suspended, or never enabled (no status), is off;
     * a refusal to answer is unknown.
     */
    @Test
    void s3SaysWhetherTheBucketKeepsVersions()
    {
        assertThat(StoreVersioning.of(s3(BucketVersioningStatus.ENABLED),
                S3_TABLE)).isEqualTo(StoreVersioning.Verdict.ON);
        assertThat(StoreVersioning.of(s3(BucketVersioningStatus.SUSPENDED),
                S3_TABLE)).isEqualTo(StoreVersioning.Verdict.OFF);
        assertThat(StoreVersioning.of(s3(null), S3_TABLE))
                .isEqualTo(StoreVersioning.Verdict.OFF);

        S3Client denied = mock(S3Client.class);
        when(denied.getBucketVersioning(
                any(GetBucketVersioningRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(403).build());
        when(denied.getBucketVersioning(
                any(Consumer.class)))
                .thenThrow(S3Exception.builder().statusCode(403).build());
        assertThat(StoreVersioning.of(io(denied), S3_TABLE))
                .isEqualTo(StoreVersioning.Verdict.UNKNOWN);
    }

    /** GCS: the bucket's versioning flag; a failure or no bucket is unknown. */
    @Test
    void gcsSaysWhetherTheBucketKeepsVersions()
    {
        assertThat(StoreVersioning.gcs(gcs(Boolean.TRUE), "gold"))
                .isEqualTo(StoreVersioning.Verdict.ON);
        assertThat(StoreVersioning.gcs(gcs(Boolean.FALSE), "gold"))
                .isEqualTo(StoreVersioning.Verdict.OFF);
        assertThat(StoreVersioning.gcs(gcs(null), "gold"))
                .isEqualTo(StoreVersioning.Verdict.OFF);

        Storage missing = mock(Storage.class);
        assertThat(StoreVersioning.gcs(missing, "gold"))
                .isEqualTo(StoreVersioning.Verdict.UNKNOWN);
        Storage failing = mock(Storage.class);
        when(failing.get(eq("gold"), any(Storage.BucketGetOption.class)))
                .thenThrow(new StorageException(403, "denied"));
        assertThat(StoreVersioning.gcs(failing, "gold"))
                .isEqualTo(StoreVersioning.Verdict.UNKNOWN);
    }

    /** Any other store, and a location with no bucket, is unknown. */
    @Test
    void otherStoresAreUnknown()
    {
        assertThat(StoreVersioning.of(new InMemoryFileIO(), S3_TABLE))
                .isEqualTo(StoreVersioning.Verdict.UNKNOWN);
        assertThat(StoreVersioning.of(s3(BucketVersioningStatus.ENABLED),
                "/var/data/wh/t")).isEqualTo(StoreVersioning.Verdict.UNKNOWN);
    }

    /**
     * An S3 file IO whose bucket answers with a status.
     *
     * @param status the status; null for a bucket never versioned
     * @return the file IO
     */
    @SuppressWarnings("unchecked")
    private static S3FileIO s3(BucketVersioningStatus status)
    {
        S3Client client = mock(S3Client.class);
        GetBucketVersioningResponse response = GetBucketVersioningResponse
                .builder().status(status).build();
        when(client.getBucketVersioning(any(Consumer.class)))
                .thenReturn(response);
        when(client.getBucketVersioning(
                any(GetBucketVersioningRequest.class))).thenReturn(response);
        return io(client);
    }

    /**
     * An S3 file IO on a client.
     *
     * @param client the client
     * @return the file IO
     */
    private static S3FileIO io(S3Client client)
    {
        S3FileIO io = new S3FileIO(() -> client);
        io.initialize(Map.of());
        return io;
    }

    /**
     * A GCS client whose bucket has a versioning flag.
     *
     * @param enabled the flag; null if unset
     * @return the client
     */
    private static Storage gcs(Boolean enabled)
    {
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.versioningEnabled()).thenReturn(enabled);
        when(storage.get(eq("gold"), any(Storage.BucketGetOption.class)))
                .thenReturn(bucket);
        return storage;
    }
}
