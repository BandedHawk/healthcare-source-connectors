/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.landing.intake.TableProgress;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole landing node on an Iceberg table intake: embedded Postgres
 * holds the source table's catalog and the landing catalog, with files in
 * memory, key files and a SEDA queue.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootTest(properties = "camel.main.run-controller=false")
@ExtendWith(OutputCaptureExtension.class)
class IcebergLandingApplicationTest
{

    /** A word in every data file's name, which must never be logged. */
    static final String SECRET_NAME = "patient-doe";

    /** The source table's schema. */
    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    /** The key files. */
    static final LandingTestKeys KEYS;

    /** The lease database and both catalogs. */
    static final EmbeddedPostgres POSTGRES;

    /** The landing catalog's warehouse. */
    static final String WAREHOUSE = "memory://wh-" + UUID.randomUUID();

    /** The source catalog's properties. */
    static final Map<String, String> SOURCE_CATALOG;

    /** The test's own catalog, to write the source table. */
    static final Catalog SOURCE;

    /** The source table. */
    static final Table TABLE;

    /** The first file, in the table before the node starts. */
    static final DataFile FIRST;

    static
    {
        try
        {
            Path root = Files.createTempDirectory("sdh-landing-table");
            KEYS = new LandingTestKeys(
                    Files.createDirectories(root.resolve("keys")));
            POSTGRES = EmbeddedPostgres.start();
            SOURCE_CATALOG = Map.of("type", "jdbc",
                    "uri", POSTGRES.getJdbcUrl("postgres", "postgres"),
                    "warehouse", "memory://source-" + UUID.randomUUID(),
                    "io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO",
                    "jdbc.user", "postgres", "jdbc.password", "postgres",
                    "jdbc.schema-version", "V1");
            SOURCE = CatalogUtil.buildIcebergCatalog("source",
                    SOURCE_CATALOG, null);
            ((SupportsNamespaces) SOURCE).createNamespace(
                    Namespace.of("ehr"));
            TABLE = SOURCE.createTable(TableIdentifier.of("ehr", "claims"),
                    SCHEMA, PartitionSpec.unpartitioned(),
                    Map.of("format-version", "2"));
            FIRST = file(1, 2, 3);
            TABLE.newAppend().appendFile(FIRST).commit();
        }
        catch (Exception failure)
        {
            throw new ExceptionInInitializerError(failure);
        }
    }

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** Puts a recording exit hook in place of the real one. */
    @TestConfiguration
    static class Overrides
    {

        /**
         * The recording hook.
         *
         * @return the hook
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }
    }

    /**
     * Points the node at the test's database, keys and table.
     *
     * @param registry the property registry
     * @throws Exception if the recipient's fingerprint can't be made
     */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception
    {
        String jdbc = POSTGRES.getJdbcUrl("postgres", "postgres");
        registry.add("sdh.landing.intake", () -> "claims");
        registry.add("sdh.landing.source.type", () -> "iceberg");
        registry.add("sdh.landing.source.access", () -> "read-only");
        registry.add("sdh.landing.source.poll-interval", () -> "200ms");
        SOURCE_CATALOG.forEach((key, value) -> registry.add(
                "sdh.landing.source.table.catalog[" + key + "]",
                () -> value));
        registry.add("sdh.landing.source.table.identifier",
                () -> "ehr.claims");
        registry.add("sdh.storage.catalog.type", () -> "jdbc");
        registry.add("sdh.storage.catalog.uri", () -> jdbc);
        registry.add("sdh.storage.catalog.warehouse", () -> WAREHOUSE);
        registry.add("sdh.storage.catalog.io-impl",
                () -> "org.apache.iceberg.inmemory.InMemoryFileIO");
        registry.add("sdh.storage.catalog[jdbc.user]", () -> "postgres");
        registry.add("sdh.storage.catalog[jdbc.password]", () -> "postgres");
        registry.add("sdh.storage.catalog[jdbc.schema-version]", () -> "V1");
        registry.add("sdh.lease.jdbc.url", () -> jdbc);
        registry.add("sdh.lease.jdbc.username", () -> "postgres");
        registry.add("sdh.lease.jdbc.password", () -> "postgres");
        registry.add("sdh.crypto.keystore.path", KEYS.keystore::toString);
        registry.add("sdh.crypto.keystore.password",
                () -> LandingTestKeys.KEYSTORE_PASSWORD);
        registry.add("sdh.crypto.keys.landed-items-kek",
                () -> LandingTestKeys.LANDED_ITEMS);
        registry.add("sdh.crypto.keys.signing.kid", () -> "landing-2026");
        registry.add("sdh.crypto.keys.signing.pem",
                KEYS.signingPem::toString);
        registry.add("sdh.crypto.keys.signing.passphrase",
                () -> LandingTestKeys.SIGNING_PASSPHRASE);
        registry.add("sdh.crypto.keys.recipient.kid",
                () -> "downstream-2026");
        registry.add("sdh.crypto.keys.recipient.pem",
                KEYS.recipientPem::toString);
        String pin = KeyMaterial.fingerprint(KEYS.recipient.getPublic());
        registry.add("sdh.crypto.keys.recipient.sha256", () -> pin);
        registry.add("sdh.queue.type", () -> "camel");
        registry.add("sdh.queue.uri", () -> "seda:sdh-out?size=1000");
        registry.add("sdh.retry.max-attempts", () -> "2");
        registry.add("sdh.retry.initial-backoff", () -> "10ms");
    }

    /** The application's Camel context. */
    @Autowired
    CamelContext camel;

    /**
     * The file the table held at the first start is landed, then each
     * append; an overwrite is reported and nothing of it landed. The
     * intake's progress is kept on the ledger, and no file name reaches
     * the log.
     *
     * @param output what the node logged
     * @throws Exception if the test fails
     */
    @Test
    void landsATablesFilesAndFollowsItsAppends(CapturedOutput output)
            throws Exception
    {
        assertThat(receive(1)).singleElement().satisfies(landed ->
        {
            assertThat(landed.kind()).isEqualTo("table-file");
            assertThat(landed.size()).isEqualTo(FIRST.fileSizeInBytes());
        });

        DataFile second = file(4, 5);
        DataFile third = file(6);
        TABLE.newAppend().appendFile(second).appendFile(third).commit();
        assertThat(receive(2)).extracting(ConnectorMessage.ItemLanded::size)
                .containsExactlyInAnyOrder(second.fileSizeInBytes(),
                        third.fileSizeInBytes());

        TABLE.newOverwrite().deleteFile(third).addFile(file(7)).commit();
        DataFile fourth = file(8);
        TABLE.newAppend().appendFile(fourth).commit();
        assertThat(receive(1)).singleElement().extracting(
                ConnectorMessage.ItemLanded::size)
                .isEqualTo(fourth.fileSizeInBytes());
        assertThat(receive(1, 1_000)).isEmpty();

        Catalog landing = CatalogUtil.buildIcebergCatalog("sdh",
                Map.of("type", "jdbc",
                        "uri", POSTGRES.getJdbcUrl("postgres", "postgres"),
                        "warehouse", WAREHOUSE,
                        "io-impl",
                        "org.apache.iceberg.inmemory.InMemoryFileIO",
                        "jdbc.user", "postgres",
                        "jdbc.password", "postgres",
                        "jdbc.schema-version", "V1"),
                null);
        Table ledger = landing.loadTable(TableIdentifier.of(
                LandingTables.DEFAULT_NAMESPACE, LandingTables.LEDGER));
        long current = TABLE.currentSnapshot().snapshotId();
        awaitTrue(() ->
        {
            ledger.refresh();
            String mark = ledger.properties().get(
                    TableProgress.PROPERTY_PREFIX + "claims");
            return mark != null && mark.contains(Long.toString(current));
        });
        ((AutoCloseable) landing).close();
        assertThat(FATAL).isEmpty();
        assertThat(output.getAll()).doesNotContain(SECRET_NAME)
                .contains("intake.table_rewrite");
    }

    /**
     * Writes a Parquet data file of rows, not yet in the table.
     *
     * @param ids the rows' IDs
     * @return the file
     * @throws Exception if it can't be written
     */
    static DataFile file(long... ids) throws Exception
    {
        String path = TABLE.location() + "/data/" + SECRET_NAME + "-"
                + UUID.randomUUID() + ".parquet";
        FileAppender<Record> appender = Parquet.write(
                TABLE.io().newOutputFile(path)).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).build();
        try (appender)
        {
            for (long id : ids)
            {
                Record row = GenericRecord.create(SCHEMA);
                row.setField("id", id);
                row.setField("name", SECRET_NAME);
                appender.add(row);
            }
        }
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(appender.length())
                .withMetrics(appender.metrics()).build();
    }

    /**
     * Waits up to a minute for item.landed messages.
     *
     * @param count how many
     * @return the messages, opened
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> receive(int count) throws Exception
    {
        return receive(count, 60_000);
    }

    /**
     * Waits for item.landed messages.
     *
     * @param count  how many
     * @param millis how long to wait
     * @return the messages, opened
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> receive(int count, long millis)
            throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream-2026", KEYS.recipient.getPrivate()),
                Map.of("landing-2026",
                        (ECPublicKey) KEYS.signing.getPublic()));
        ConsumerTemplate consumer = camel.createConsumerTemplate();
        List<ConnectorMessage.ItemLanded> landed = new ArrayList<>();
        long deadline = System.nanoTime() + millis * 1_000_000L;
        while (landed.size() < count && System.nanoTime() < deadline)
        {
            String body = consumer.receiveBody("seda:sdh-out", 200,
                    String.class);
            if (body != null)
            {
                landed.add((ConnectorMessage.ItemLanded)
                        opener.open(body.getBytes(UTF_8)));
            }
        }
        return landed;
    }

    /**
     * Waits up to 30 seconds for a condition.
     *
     * @param condition the condition
     * @throws Exception if interrupted
     */
    static void awaitTrue(BooleanSupplier condition) throws Exception
    {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline)
        {
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
