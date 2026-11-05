/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/**
 * Tests for {@link ConnectorProperties}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConnectorPropertiesTest
{

    /**
     * Nested YAML keys bind as dotted catalog properties and split
     * options, and unset settings take their defaults.
     */
    @Test
    void nestedYamlKeysBindAsDottedCatalogPropertiesAndSplitOptions()
    {
        // What a YAML file like design §14 flattens to.
        Map<String, String> yaml = Map.of(
                "sdh.split.format", "csv",
                "sdh.split.options.csv.header", "true",
                "sdh.split.options.namespace", "urn:hl7-org:v3",
                "sdh.storage.catalog.type", "rest",
                "sdh.storage.catalog.s3.endpoint", "https://seaweedfs:8333",
                "sdh.storage.catalog.s3.path-style-access", "true",
                "sdh.storage.catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
                "sdh.lease.ttl", "90s");
        ConnectorProperties properties =
                new Binder(new MapConfigurationPropertySource(yaml))
                        .bindOrCreate("sdh", ConnectorProperties.class);
        assertThat(properties.split().options())
                .containsEntry("csv.header", "true")
                .containsEntry("namespace", "urn:hl7-org:v3");
        assertThat(properties.storage().catalog())
                .containsEntry("s3.endpoint", "https://seaweedfs:8333")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        assertThat(properties.lease().ttl()).isEqualTo(Duration.ofSeconds(90));
        assertThat(properties.lease().renewInterval())
                .isEqualTo(Duration.ofSeconds(15));
        assertThat(properties.storage().commit().maxSections())
                .isEqualTo(10_000);
        assertThat(properties.landing().idleWait())
                .isEqualTo(Duration.ofSeconds(5));
        assertThat(properties.landing().storage().namespace())
                .isEqualTo("sdh_landing");
        assertThat(properties.landing().reconcile().enabled()).isTrue();
        assertThat(properties.landing().reconcile().grace())
                .isEqualTo(Duration.ofMinutes(10));
        assertThat(properties.landing().reconcile().interval())
                .isEqualTo(Duration.ofMinutes(15));
    }
}
