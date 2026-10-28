package com.sdh.healthcare.connector.file.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class ConnectorPropertiesTest {

    @Test
    void nestedYamlKeysBindAsDottedCatalogPropertiesAndSplitOptions() {
        // What a YAML file like design §14 flattens to.
        Map<String, String> yaml = Map.of(
                "sdh.split.format", "csv",
                "sdh.split.options.csv.header", "true",
                "sdh.split.options.namespace", "urn:hl7-org:v3",
                "sdh.storage.catalog.type", "rest",
                "sdh.storage.catalog.s3.endpoint", "https://seaweedfs:8333",
                "sdh.storage.catalog.s3.path-style-access", "true",
                "sdh.storage.catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                "sdh.lease.ttl", "90s");
        ConnectorProperties p = new Binder(new MapConfigurationPropertySource(yaml))
                .bindOrCreate("sdh", ConnectorProperties.class);
        assertThat(p.split().options()).containsEntry("csv.header", "true").containsEntry("namespace", "urn:hl7-org:v3");
        assertThat(p.storage().catalog()).containsEntry("s3.endpoint", "https://seaweedfs:8333")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        assertThat(p.lease().ttl()).isEqualTo(Duration.ofSeconds(90));
        assertThat(p.lease().renewInterval()).isEqualTo(Duration.ofSeconds(15));
        assertThat(p.storage().commit().maxSections()).isEqualTo(10_000);
        assertThat(p.file().readLock()).isEqualTo("changed");
    }
}
