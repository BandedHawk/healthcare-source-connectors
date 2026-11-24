/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.core.maintenance.TableRestore;
import com.sdh.healthcare.connector.core.maintenance.VersionedFileIO;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.DataAdmin;
import com.sdh.healthcare.connector.stage.admin.DataCommands;
import com.sdh.healthcare.connector.stage.admin.Operation;
import com.sdh.healthcare.connector.stage.admin.RestoreCommands;
import com.sdh.healthcare.connector.stage.control.ControlRoute;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import javax.crypto.KeyGenerator;
import javax.sql.DataSource;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A restore's commands (DESIGN.md §18.7): {@code restore-check} reports
 * and changes nothing; {@code restore-repair} brings back the files the
 * restored catalog needs, brings the landing node's tables forward and
 * raises the leases' epochs, keeps the orphans, and makes the purges and
 * erasures again that the store's control log holds; {@code
 * restore-release} lets maintenance have the orphans back. Each refuses
 * while a lease is live.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RestoreCommandsTest
{

    /** The stage's KEK. */
    static final String KEK = "records-kek/1";

    /** The scope. */
    static final String SCOPE = "convert/test";

    /** The stage's table's columns. */
    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Holds the KEK. */
    AesKeyWrapProvider keys;

    /** The schema of this test's rows. */
    String schema;

    /** The scope's state. */
    ControlState state;

    /** The scope's purges. */
    PurgeRequests purges;

    /** The stage's catalog. */
    JdbcCatalog catalog;

    /** The stage's table. */
    TableIdentifier id;

    /** Creates the tests; JUnit runs each test on a new instance. */
    RestoreCommandsTest()
    {
    }

    /**
     * Makes the KEK, a schema with a lease table, and a catalog over a
     * versioned store, each the test's own.
     *
     * @throws Exception if they can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
        schema = StageTestSupport.freshSchema();
        state = new ControlState(StageTestSupport.database(), schema, SCOPE);
        purges = new PurgeRequests(StageTestSupport.database(), schema,
                SCOPE);
        sql("CREATE SCHEMA IF NOT EXISTS " + schema);
        sql("CREATE TABLE " + schema + ".sdh_lease (name text PRIMARY KEY,"
                + " owner text NOT NULL, epoch bigint NOT NULL, expires_at"
                + " timestamptz NOT NULL, acquired_at timestamptz NOT NULL,"
                + " renewed_at timestamptz NOT NULL)");
        catalog = new JdbcCatalog();
        catalog.initialize("jdbc_" + UUID.randomUUID().toString()
                .replace("-", ""), Map.of(CatalogProperties.URI,
                        StageTestSupport.POSTGRES.getJdbcUrl("postgres",
                                "postgres"),
                        CatalogProperties.WAREHOUSE_LOCATION,
                        "memory://wh-" + UUID.randomUUID(),
                        CatalogProperties.FILE_IO_IMPL,
                        VersionedFileIO.class.getName(),
                        "jdbc.schema-version", "V1"));
        catalog.createNamespace(Namespace.of("ns"));
        id = TableIdentifier.of("ns", "records");
    }

    /**
     * Restored to an earlier point, the stage's catalog needs a file its
     * maintenance deleted since. The check names it and changes nothing;
     * the repair, refused while a lease is live, brings it back and keeps
     * the orphans; the release lets maintenance have them.
     *
     * @throws Exception if the test fails
     */
    @Test
    void repairsWhatTheRestoredCatalogNeeds() throws Exception
    {
        Table table = catalog.createTable(id, SCHEMA,
                PartitionSpec.unpartitioned());
        DataFile first = append(table, "a");
        String point = current(table);
        append(table, "b");
        table.io().deleteFile(first.location());
        restore(point);
        Context context = new Context(false, keys);

        String checked = run(context, "restore-check");
        assertThat(checked).contains(SCOPE + ": checked; NOT whole")
                .contains("1 missing").contains("missing: "
                        + first.location());
        assertThat(table.io().newInputFile(first.location()).exists())
                .isFalse();

        lease("convert", 1, true);
        assertThatThrownBy(() -> run(context, "restore-repair", "--ticket",
                "INC-4")).isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("1 lease(s)");
        assertThatThrownBy(() -> run(context, "restore-check"))
                .isInstanceOf(AdminArgs.UsageException.class);
        sql("UPDATE " + schema + ".sdh_lease SET expires_at = now()"
                + " - interval '1 minute'");
        assertThatThrownBy(() -> run(context, "restore-repair"))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("ticket");

        String repaired = run(context, "restore-repair", "--ticket",
                "INC-4");
        assertThat(repaired).contains(SCOPE + ": repaired; whole, may start")
                .contains("1 brought back, 0 missing")
                .contains("Not paused, as restored");
        assertThat(table.io().newInputFile(first.location()).exists())
                .isTrue();
        assertThat(catalog.loadTable(id).properties())
                .containsKey(TableRestore.KEEP_ORPHANS);
        assertThat(run(context, "restore-check")).contains("whole, may"
                + " start");

        assertThat(run(context, "restore-release", "--ticket", "INC-4"))
                .contains("Released: " + id);
        assertThat(catalog.loadTable(id).properties())
                .doesNotContainKey(TableRestore.KEEP_ORPHANS);
        assertThat(run(context, "restore-release", "--ticket", "INC-4"))
                .contains("No table was keeping its orphans");

        assertThat(approval("restore-check"))
                .isEqualTo(Operation.Approval.NONE);
        assertThat(approval("restore-repair"))
                .isEqualTo(Operation.Approval.FOUR_EYES);
        assertThat(approval("restore-release"))
                .isEqualTo(Operation.Approval.FOUR_EYES);
        assertThat(ControlRoute.all().stream().map(ControlRoute::key))
                .contains("GET /api/v1/restore/check",
                        "POST /api/v1/restore/repair",
                        "POST /api/v1/restore/release");
        try (InputStream spec = getClass().getClassLoader()
                .getResourceAsStream("control/openapi-v1.yaml"))
        {
            assertThat(new String(spec.readAllBytes(),
                    StandardCharsets.UTF_8)).contains(
                            "operationId: restoreCheck",
                            "operationId: restoreRepair",
                            "operationId: restoreRelease");
        }
    }

    /**
     * The landing node's table, restored to an earlier point, is brought
     * forward to its newest metadata, so no item landed since is lost, and
     * the leases' epochs are raised to the one it records, so the next
     * holder's commits aren't fenced out. A higher epoch is left alone.
     *
     * @throws Exception if the test fails
     */
    @Test
    void bringsTheLandingNodeForward() throws Exception
    {
        Table table = catalog.createTable(id, SCHEMA,
                PartitionSpec.unpartitioned());
        append(table, "a");
        String point = current(table);
        append(table, "b");
        table.updateProperties().set(FencedCommitter.EPOCH_PROPERTY, "5")
                .commit();
        String tip = current(table);
        restore(point);
        lease("landing", 3, false);
        lease("other", 9, false);
        Context context = new Context(true, keys);

        assertThat(run(context, "restore-check")).contains(": forward")
                .contains("forward to " + tip);
        assertThat(current(catalog.loadTable(id))).isEqualTo(point);

        assertThat(run(context, "restore-repair", "--ticket", "INC-5"))
                .contains("whole, may start")
                .contains("Leases raised to epoch 5");
        // At the tip, and one commit on: the hold on its orphans.
        Table forward = catalog.loadTable(id);
        assertThat(((HasTableOperations) forward).operations().current()
                .previousFiles()).anySatisfy(entry ->
                        assertThat(entry.file()).isEqualTo(tip));
        assertThat(forward.properties())
                .containsEntry(FencedCommitter.EPOCH_PROPERTY, "5")
                .containsKey(TableRestore.KEEP_ORPHANS);
        assertThat(epoch("landing")).isEqualTo(5);
        assertThat(epoch("other")).isEqualTo(9);
        assertThat(run(context, "restore-check")).contains(": checked,")
                .doesNotContain("forward to");
    }

    /**
     * The purges and erasure the store's control log holds and the
     * restored database doesn't are made again by a repair, so purged
     * records stay purged; the rotations and retention changes since are
     * left for the operator, the latest of each kind deciding. A purge
     * asked through the command is kept in the log.
     *
     * @throws Exception if the test fails
     */
    @Test
    void makesTheControlChangesAgain() throws Exception
    {
        catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        Context context = new Context(false, keys);
        UUID asked = UUID.randomUUID();
        run(context, "purge", "--item-id", asked.toString(), "--ticket",
                "CHG-1", "--wait", "0");
        ControlLog log = context.controlLog().orElseThrow();
        assertThat(log.entries()).singleElement().satisfies(entry ->
        {
            assertThat(entry.kind()).isEqualTo(ControlLog.Kind.PURGE);
            assertThat(entry.field("item")).isEqualTo(asked.toString());
            assertThat(entry.field("ticket")).isEqualTo("CHG-1");
        });

        UUID lost = UUID.randomUUID();
        log.keep(ControlLog.Kind.PURGE, "op-purge", "bob",
                Map.of("item", lost.toString(), "ticket", "CHG-2"));
        log.keep(ControlLog.Kind.ROTATE, "op-rotate", "bob",
                Map.of("from", KEK, "to", "records-kek/2"));
        log.keep(ControlLog.Kind.RETENTION, "op-ret-1", "bob",
                Map.of("retention", "P60D"));
        log.keep(ControlLog.Kind.RETENTION, "op-ret-2", "bob",
                Map.of("retention", "P90D"));
        log.keep(ControlLog.Kind.ERASE, "op-erase", "bob",
                Map.of("ticket", "CHG-3"));

        String checked = run(context, "restore-check");
        assertThat(checked).contains("NOT whole")
                .contains("purge op-purge").contains("to-replay")
                .contains("rotate op-rotate").contains("to-redo")
                .contains("retention op-ret-1").contains("superseded")
                .contains("erase op-erase");
        assertThat(purges.get("op-purge")).isEmpty();
        assertThat(state.erased()).isEmpty();

        String repaired = run(context, "restore-repair", "--ticket",
                "INC-6");
        assertThat(repaired).contains("purge op-purge").contains("replayed")
                .contains("NOT whole");
        assertThat(purges.get("op-purge")).hasValueSatisfying(purge ->
                assertThat(purge.itemId()).isEqualTo(lost));
        assertThat(state.erased()).hasValueSatisfying(erased ->
                assertThat(erased.id()).isEqualTo("op-erase"));
        assertThat(run(context, "restore-check"))
                .doesNotContain("purge op-purge")
                .doesNotContain("erase op-erase")
                .contains("rotate op-rotate");
    }

    /**
     * A KEK the provider can't use is reported, so the stage isn't whole:
     * its key custodian enables it before the stage starts. The report
     * names the KEK and the failure's kind, never its text.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reportsAKekThatCantBeUsed() throws Exception
    {
        catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        Context context = new Context(false, new AesKeyWrapProvider(
                Map.of("other-kek/1", generator.generateKey())));
        assertThat(run(context, "restore-check")).contains("NOT whole")
                .contains("KEK " + KEK + " can't be used (")
                .contains("ask the key custodian to enable it");
        assertThat(run(new Context(false, keys), "restore-check"))
                .contains("whole, may start").doesNotContain("KEK ");

        // A manifest missing until the repair: the KEKs the state and
        // settings name are tried, and the tables' own reported unread.
        Context unread = new Context(false, keys)
        {
            @Override
            public List<Table> keyTables()
            {
                throw new UncheckedIOException(new IOException("gone"));
            }
        };
        assertThat(run(unread, "restore-check")).contains("NOT whole")
                .contains("The KEKs the tables record can't be read"
                        + " (UncheckedIOException)")
                .doesNotContain("KEK " + KEK);
    }

    /**
     * Runs a command as {@code alice}.
     *
     * @param context the stage's context
     * @param args    the command and its arguments
     * @return what it printed
     * @throws Exception if it fails
     */
    String run(Context context, String... args) throws Exception
    {
        AdminCommand<Context> command = command(args[0]);
        AdminArgs parsed = AdminArgs.parse(List.of(args), command.flags())
                .withInput(new ByteArrayInputStream(new byte[0]));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        command.run(parsed, context, new AuditRecord("alice"),
                new PrintStream(out, true, StandardCharsets.UTF_8));
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * A restore or data command.
     *
     * @param name its name
     * @return it
     */
    static AdminCommand<Context> command(String name)
    {
        return Stream.concat(RestoreCommands.<Context>all().stream(),
                DataCommands.<Context>all().stream())
                .filter(each -> each.name().equals(name)).findFirst()
                .orElseThrow();
    }

    /**
     * The approval a command's requests need.
     *
     * @param name the command's name
     * @return it
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static Operation.Approval approval(String name)
    {
        return ((Operation) command(name)).approval(null);
    }

    /**
     * Points the catalog back at earlier metadata, as restoring its
     * database does.
     *
     * @param metadata the earlier metadata
     */
    void restore(String metadata)
    {
        catalog.dropTable(id, false);
        catalog.registerTable(id, metadata);
    }

    /**
     * A lease.
     *
     * @param name  its name
     * @param epoch its epoch
     * @param live  whether it is live
     * @throws Exception if it can't be written
     */
    void lease(String name, long epoch, boolean live) throws Exception
    {
        sql("INSERT INTO " + schema + ".sdh_lease VALUES ('" + name
                + "', 'pod-1', " + epoch + ", now() " + (live ? "+" : "-")
                + " interval '1 minute', now(), now())");
    }

    /**
     * A lease's epoch.
     *
     * @param name its name
     * @return its epoch
     * @throws Exception if it can't be read
     */
    long epoch(String name) throws Exception
    {
        try (Connection connection = StageTestSupport.database()
                .getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT epoch FROM "
                        + schema + ".sdh_lease WHERE name = '" + name + "'"))
        {
            rows.next();
            return rows.getLong(1);
        }
    }

    /**
     * Runs a statement.
     *
     * @param statement it
     * @throws Exception if it fails
     */
    static void sql(String statement) throws Exception
    {
        try (Connection connection = StageTestSupport.database()
                .getConnection();
                Statement each = connection.createStatement())
        {
            each.execute(statement);
        }
    }

    /**
     * Appends a data file, written to the store.
     *
     * @param table the table
     * @param name  the file's name
     * @return it
     */
    static DataFile append(Table table, String name)
    {
        String location = table.location() + "/data/" + name + "-"
                + UUID.randomUUID() + ".parquet";
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = table.io().newOutputFile(location).create())
        {
            out.write(bytes);
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(location).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(bytes.length).withRecordCount(1)
                .build();
        table.newAppend().appendFile(file).commit();
        return file;
    }

    /**
     * The table's current metadata file.
     *
     * @param table the table
     * @return its location
     */
    static String current(Table table)
    {
        return ((HasTableOperations) table).operations().refresh()
                .metadataFileLocation();
    }

    /** The stage, as the restore commands see it: one table. */
    class Context implements DataAdmin
    {

        /** Whether it is the landing node. */
        final boolean landing;

        /** Holds its KEKs. */
        final KeyProvider provider;

        /** Its signing key. */
        final KeyPair signing;

        /**
         * Creates the context.
         *
         * @param landing  whether it is the landing node
         * @param provider holds its KEKs
         * @throws Exception if the signing key can't be made
         */
        Context(boolean landing, KeyProvider provider) throws Exception
        {
            this.landing = landing;
            this.provider = provider;
            this.signing = StageTestSupport.ec();
        }

        @Override
        public ControlState controlState(boolean change)
        {
            return state;
        }

        @Override
        public String settingKek()
        {
            return KEK;
        }

        @Override
        public KeyProvider keyProvider()
        {
            return provider;
        }

        @Override
        public List<Table> keyTables()
        {
            return List.of();
        }

        @Override
        public List<WrappedRows> wrappedRows()
        {
            return List.of();
        }

        @Override
        public DataSource keyDatabase()
        {
            return StageTestSupport.database();
        }

        @Override
        public String stage()
        {
            return "convert";
        }

        @Override
        public RetentionPolicy retentionPolicy()
        {
            return RetentionPolicy.of(Duration.ofDays(30),
                    Duration.ofDays(1), new CommonSettings.RetentionBounds(
                            Duration.ofDays(7), Duration.ofDays(365)));
        }

        @Override
        public PurgeRequests purgeRequests(boolean change)
        {
            return purges;
        }

        @Override
        public List<DataAdmin.Erasable> erasable()
        {
            return List.of(new DataAdmin.Erasable(catalog, id));
        }

        @Override
        public DataSource dataToChange()
        {
            return StageTestSupport.database();
        }

        @Override
        public KeyMaterial signingKeys() throws Exception
        {
            return new KeyMaterial(keys, new ECDSASigner(
                    (ECPrivateKey) signing.getPrivate()), null, "stage-key",
                    null, null, null, null, List.of());
        }

        @Override
        public AuditTables auditTables()
        {
            return new AuditTables(schema);
        }

        @Override
        public Optional<String> leaseTable()
        {
            return Optional.of(schema + ".sdh_lease");
        }

        @Override
        public boolean bringsForward()
        {
            return landing;
        }
    }
}
