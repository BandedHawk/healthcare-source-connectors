/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BucketSettings}, and bucket sources in
 * {@link SourceSettings}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BucketSettingsTest
{

    /**
     * Bucket settings as bound.
     *
     * @param endpoint         the endpoint
     * @param region           the region
     * @param accessKeyId      the access key
     * @param secret           its secret
     * @param connectionString the Azure connection string
     * @param projectId        the GCS project
     * @param anonymous        whether to send no credentials
     * @return the settings
     */
    static LandingProperties.Bucket bucket(String endpoint, String region,
            String accessKeyId, String secret, String connectionString,
            String projectId, boolean anonymous)
    {
        return new LandingProperties.Bucket("b", endpoint, region, true,
                accessKeyId, secret, connectionString, projectId, anonymous,
                Duration.ofSeconds(15), Duration.ofSeconds(60));
    }

    /**
     * A source's settings, with a bucket.
     *
     * @param path   the source's path
     * @param type   the source's type
     * @param bucket the bucket
     * @return the settings as bound
     */
    static LandingProperties.Source source(String path, String type,
            LandingProperties.Bucket bucket)
    {
        return new LandingProperties.Source(Path.of(path), "read-only", null,
                null, ".landed", "settled", Duration.ZERO, "*", false,
                Duration.ofSeconds(10), type, Duration.ofMinutes(5), null,
                null, bucket, null);
    }

    /**
     * Each store's type takes a bucket; its path is an absolute key
     * prefix.
     */
    @Test
    void readsEachStoresType()
    {
        SourceSettings s3 = SourceSettings.of(source("/in", "s3",
                bucket(null, "eu-west-1", null, null, null, null, false)));
        assertThat(s3.type()).isEqualTo(SourceSettings.Type.S3);
        assertThat(s3.remote()).isTrue();
        assertThat(s3.bucket().name()).isEqualTo("b");
        assertThat(SourceSettings.of(source("/", "azure-blob",
                bucket("https://a.blob.core.windows.net", null, null, null,
                        null, null, false))).type())
                .isEqualTo(SourceSettings.Type.AZURE_BLOB);
        assertThat(SourceSettings.of(source("/", "gcs",
                bucket(null, null, null, null, null, "p", false))).bucket()
                .projectId()).isEqualTo("p");
        assertThatThrownBy(() -> SourceSettings.of(source("in", "s3",
                bucket(null, null, null, null, null, null, false))))
                .hasMessageContaining("absolute");
        assertThatThrownBy(() -> SourceSettings.of(source("/in", "s3",
                new LandingProperties.Bucket(null, null, null, false, null,
                        null, null, null, false, Duration.ofSeconds(1),
                        Duration.ofSeconds(1)))))
                .hasMessageContaining("bucket.name");
    }

    /** S3 takes an access key with its secret, or none. */
    @Test
    void checksS3Credentials()
    {
        BucketSettings keyed = BucketSettings.of(SourceSettings.Type.S3,
                bucket("http://seaweed:8333", null, "AK", "s3cret", null,
                        null, false));
        assertThat(keyed.region()).isEqualTo("us-east-1");
        assertThat(keyed.toString()).doesNotContain("s3cret");
        assertThatThrownBy(() -> BucketSettings.of(SourceSettings.Type.S3,
                bucket(null, null, "AK", null, null, null, false)))
                .hasMessageContaining("go together");
        assertThatThrownBy(() -> BucketSettings.of(SourceSettings.Type.S3,
                bucket(null, null, "AK", "s", null, null, true)))
                .hasMessageContaining("anonymous");
        assertThatThrownBy(() -> BucketSettings.of(SourceSettings.Type.S3,
                bucket(null, null, null, null, "cs", null, false)))
                .hasMessageContaining("only for type azure-blob");
    }

    /**
     * Azure takes a connection string or an endpoint, not both, and never
     * shows the connection string.
     */
    @Test
    void checksAzureCredentials()
    {
        assertThatThrownBy(() -> BucketSettings.of(
                SourceSettings.Type.AZURE_BLOB,
                bucket(null, null, null, null, null, null, false)))
                .hasMessageContaining("one of");
        assertThatThrownBy(() -> BucketSettings.of(
                SourceSettings.Type.AZURE_BLOB,
                bucket("https://a", null, null, null, "cs", null, false)))
                .hasMessageContaining("one of");
        assertThatThrownBy(() -> BucketSettings.of(
                SourceSettings.Type.AZURE_BLOB,
                bucket(null, "eu-west-1", null, null, "cs", null, false)))
                .hasMessageContaining("only for type s3");
        assertThat(BucketSettings.of(SourceSettings.Type.AZURE_BLOB,
                bucket(null, null, null, null, "AccountKey=s3cret", null,
                        false)).toString()).doesNotContain("s3cret");
        assertThat(bucket(null, null, "AK", "s3cret", "AccountKey=x", null,
                false).toString()).doesNotContain("s3cret", "AccountKey");
    }

    /** The endpoint must be an http or https address. */
    @Test
    void checksTheEndpoint()
    {
        assertThatThrownBy(() -> BucketSettings.of(SourceSettings.Type.GCS,
                bucket("ftp://x", null, null, null, null, null, false)))
                .hasMessageContaining("http or https");
        assertThatThrownBy(() -> BucketSettings.of(SourceSettings.Type.GCS,
                bucket("not a uri", null, null, null, null, null, false)))
                .hasMessageContaining("http or https");
    }
}
