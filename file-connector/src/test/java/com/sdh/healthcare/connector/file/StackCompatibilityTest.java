package com.sdh.healthcare.connector.file;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.storage.SectionBatchWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.KeyGenerator;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Spike §16.2: Spring Boot 4.1 + Camel 4.22 LTS with every broker component and the Iceberg/Parquet
 * stack on one classpath, on Java 25.
 */
@SpringBootTest(properties = {"sdh.file.min-age=1s", "camel.springboot.main-run-controller=false"})
class StackCompatibilityTest {
    static final Path INBOUND;

    static {
        try {
            INBOUND = Files.createTempDirectory("inbound");
            Files.createDirectories(INBOUND.resolve("completed"));
            Files.createDirectories(INBOUND.resolve("failed"));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("sdh.file.inbound-dir", INBOUND::toString);
    }

    @TestConfiguration
    static class PipelineStub {
        @Bean
        RouteBuilder pipelineStub() {
            return new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:pipeline").to("mock:pipeline");
                }
            };
        }
    }

    @Autowired
    CamelContext camel;

    @Test
    void allBrokerComponentsResolve() {
        for (String c : new String[] {"kafka", "aws2-sqs", "google-pubsub", "azure-servicebus", "azure-eventhubs",
                "jms", "spring-rabbitmq", "file"}) {
            assertThat(camel.getComponent(c)).as(c).isNotNull();
        }
    }

    @Test
    void picksUpSettledFilesOldestFirstAndIgnoresSubdirectories() throws Exception {
        MockEndpoint mock = camel.getEndpoint("mock:pipeline", MockEndpoint.class);
        mock.expectedMessageCount(2);
        Files.writeString(INBOUND.resolve("failed").resolve("x.json"), "{}");
        Path older = Files.writeString(INBOUND.resolve("b.xml"), "<b/>");
        Files.setLastModifiedTime(older, java.nio.file.attribute.FileTime.from(Instant.now().minusSeconds(60)));
        Files.writeString(INBOUND.resolve("a.xml"), "<a/>");
        mock.setResultWaitTime(TimeUnit.SECONDS.toMillis(20));
        mock.assertIsSatisfied();
        assertThat(mock.getExchanges()).extracting(e -> e.getIn().getHeader(Exchange.FILE_NAME_ONLY, String.class))
                .containsExactly("b.xml", "a.xml");
        assertThat(Files.exists(older)).as("Camel must not move or delete").isTrue();
    }

    @Test
    void icebergParquetStackWorksInsideBootClasspath() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("boot", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://boot-" + UUID.randomUUID()));
        Table table = IngestTables.sections(catalog, "sdh_ingest");
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return 1; }
            public void checkHeld() {}
        });
        committer.establish(table);
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        SectionBatchWriter w = new SectionBatchWriter(table, new AesKeyWrapProvider(Map.of("k", kg.generateKey())),
                "k", new SectionIds(new byte[32]), Instant.now());
        w.add(new SectionBatchWriter.SectionInput(UUID.randomUUID(), 0, 0, "csv",
                "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8)));
        DataFile df = w.finish();
        committer.commit(table, tx -> tx.newFastAppend().appendFile(df).commit());
        assertThat(table.currentSnapshot().summary()).containsEntry("total-records", "1");
        assertThat(com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION.toString()).startsWith("2.22");
    }
}
