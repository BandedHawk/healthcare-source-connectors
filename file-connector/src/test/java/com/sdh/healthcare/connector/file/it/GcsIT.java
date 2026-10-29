package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Google Cloud Storage through Iceberg's {@code GCSFileIO} (design §8.1), against fake-gcs-server. The client
 * reaches the emulator with {@code gcs.service.host} and no credentials ({@code gcs.no-auth}); in production the
 * default Google credentials (workload identity or a service account) are used instead.
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
@Import(ObjectStoreItBase.Overrides.class)
class GcsIT extends ObjectStoreItBase {
    static final String BUCKET = "sdh-warehouse";
    static final GenericContainer<?> GCS;
    static final Fixture FIXTURE;

    static {
        ItSupport.requireDocker();
        GCS = new GenericContainer<>("fsouza/fake-gcs-server:1.56.1")
                .withCommand("-scheme", "http", "-port", "4443", "-backend", "memory")
                .withExposedPorts(4443)
                .waitingFor(Wait.forHttp("/storage/v1/b").forPort(4443).withStartupTimeout(Duration.ofMinutes(2)));
        GCS.start();
        try {
            // Resumable uploads return a URL built from the server's external URL, which must be the address this
            // JVM uses; it is only known once the port is mapped.
            http("PUT", "/_internal/config", "{\"externalUrl\":\"" + endpoint() + "\"}", 200);
            http("POST", "/storage/v1/b?project=sdh-it", "{\"name\":\"" + BUCKET + "\"}", 200);
            Map<String, String> store = new LinkedHashMap<>();
            store.put("warehouse", "gs://" + BUCKET + "/wh");
            store.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");
            store.put("gcs.service.host", endpoint());
            store.put("gcs.no-auth", "true");
            store.put("gcs.project-id", "sdh-it");
            FIXTURE = Fixture.create("gcs", store);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static String endpoint() {
        return "http://" + GCS.getHost() + ":" + GCS.getMappedPort(4443);
    }

    static void http(String method, String path, String json, int expected) throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(endpoint() + path))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).as(method + " " + path + ": " + r.body()).isEqualTo(expected);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        FIXTURE.register(r);
    }

    @Override
    Fixture fixture() {
        return FIXTURE;
    }

    @Override
    List<String> list(String location) {
        var io = (SupportsPrefixOperations) runtime.sections().io();
        return StreamSupport.stream(io.listPrefix(location).spliterator(), false).map(FileInfo::location).toList();
    }

    @Override
    boolean orphanCleanupSupported() {
        return true;
    }
}
