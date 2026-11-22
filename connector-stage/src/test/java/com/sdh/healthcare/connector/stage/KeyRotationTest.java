/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.landing.SourceRef;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.Rekey;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.KeyAdmin;
import com.sdh.healthcare.connector.stage.admin.KeyCommands;
import com.sdh.healthcare.connector.stage.admin.Operation;
import com.sdh.healthcare.connector.stage.control.ControlRoute;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import javax.crypto.KeyGenerator;
import javax.sql.DataSource;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Rotating a stage's KEK and re-keying what it stores
 * (CONTROL-API-DESIGN.md §11): the state kept, the watch that gives each
 * new batch its KEK, the database rows re-wrapped, the maintenance pass
 * that carries a re-key out, and the commands.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KeyRotationTest
{

    /** The KEK the stage starts with: its setting's. */
    static final String OLD_KEK = "records-kek/1";

    /** The KEK rotated to. */
    static final String NEW_KEK = "records-kek/2";

    /** A KEK the stage can't use. */
    static final String UNUSABLE = "records-kek/9";

    /** The stage whose rows are re-keyed. */
    static final String STAGE = "convert";

    /** The scope. */
    static final String SCOPE = "convert/test";

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Holds both KEKs. */
    AesKeyWrapProvider keys;

    /** The schema of this test's tables. */
    String schema;

    /** The scope's state. */
    ControlState state;

    /** Creates the tests; JUnit runs each test on a new instance. */
    KeyRotationTest()
    {
    }

    /**
     * Makes the KEKs, and a schema of the test's own.
     *
     * @throws Exception if the KEKs can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(OLD_KEK, generator.generateKey(),
                NEW_KEK, generator.generateKey()));
        schema = StageTestSupport.freshSchema();
        state = new ControlState(StageTestSupport.database(), schema, SCOPE);
    }

    /**
     * Nothing is rotated until it is, and each scope is its own. A re-key
     * asked for is kept with who and when; asked again by its operation,
     * it is the same; another is refused until it ends; progress adds up;
     * once ended, another may be asked for.
     */
    @Test
    void keepsTheRotationAndTheReKeyAsked()
    {
        assertThat(state.kek()).isEmpty();
        assertThat(state.rekeying()).isEmpty();
        ControlState.Kek rotated = state.rotate("olga", NEW_KEK);
        assertThat(rotated.kek()).isEqualTo(NEW_KEK);
        assertThat(rotated.setBy()).isEqualTo("olga");
        assertThat(rotated.setAt()).isNotNull();
        assertThat(state.kek()).contains(rotated);
        assertThat(new ControlState(StageTestSupport.database(), schema,
                "transform/test").kek()).isEmpty();
        assertThat(state.paused()).isEmpty();

        ControlState.Rekeying asked = state.requestRekey("op-1", "olga",
                OLD_KEK, NEW_KEK);
        assertThat(asked.state()).isEqualTo(ControlState.RekeyState.REQUESTED);
        assertThat(asked.requestedBy()).isEqualTo("olga");
        assertThat(state.requestRekey("op-1", "oscar", OLD_KEK, NEW_KEK)
                .requestedBy()).isEqualTo("olga");
        assertThatThrownBy(() -> state.requestRekey("op-2", "oscar",
                OLD_KEK, NEW_KEK)).isInstanceOf(IllegalStateException.class);

        assertThat(state.rekeyAdvanced("op-1", 2, 30)).isTrue();
        assertThat(state.rekeyAdvanced("op-1", 1, 5)).isTrue();
        assertThat(state.rekeyAdvanced("op-2", 1, 5)).isFalse();
        ControlState.Rekeying running = state.rekeying().orElseThrow();
        assertThat(running.state()).isEqualTo(ControlState.RekeyState.RUNNING);
        assertThat(running.files()).isEqualTo(3);
        assertThat(running.rows()).isEqualTo(35);

        assertThat(state.rekeyEnded("op-1", ControlState.RekeyState.DONE,
                null)).isTrue();
        assertThat(state.rekeyEnded("op-1", ControlState.RekeyState.FAILED,
                "late")).isFalse();
        assertThat(state.rekeying().orElseThrow().state())
                .isEqualTo(ControlState.RekeyState.DONE);
        assertThat(state.requestRekey("op-1", "oscar", OLD_KEK, NEW_KEK)
                .state()).isEqualTo(ControlState.RekeyState.DONE);
        assertThat(state.requestRekey("op-2", "oscar", NEW_KEK, OLD_KEK)
                .files()).isZero();

        assertThatThrownBy(() -> state.requestRekey("op-3", "oscar",
                OLD_KEK, OLD_KEK))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> state.rekeyEnded("op-2",
                ControlState.RekeyState.RUNNING, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> state.rotate("olga", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A table C6 made, with only the pause's columns, gains the KEK's and
     * the re-key's, and keeps its pause.
     *
     * @throws Exception if the test fails
     */
    @Test
    void bringsATableC6MadeUpToDate() throws Exception
    {
        try (Connection connection = StageTestSupport.database()
                .getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("CREATE TABLE " + schema + ".sdh_control_state"
                    + " (scope text PRIMARY KEY, paused boolean NOT NULL,"
                    + " paused_by text, paused_at timestamptz, reason text,"
                    + " updated_at timestamptz NOT NULL)");
        }
        state.pause("olga", "Upgrade window");
        assertThat(state.rotate("olga", NEW_KEK).kek()).isEqualTo(NEW_KEK);
        assertThat(state.requestRekey("op-1", "olga", OLD_KEK, NEW_KEK)
                .state()).isEqualTo(ControlState.RekeyState.REQUESTED);
        assertThat(state.paused()).isPresent();
    }

    /**
     * The watch gives the setting's KEK until rotated. A KEK the pod can't
     * use is refused and audited once, and the one in use kept; one it
     * can is taken and audited; a read that fails keeps it.
     */
    @Test
    void takesARotatedKekOnlyOnceItIsProbed()
    {
        AtomicReference<Optional<String>> rotated =
                new AtomicReference<>(Optional.empty());
        AtomicReference<RuntimeException> unreadable =
                new AtomicReference<>();
        KekWatch watch = new KekWatch(() ->
        {
            if (unreadable.get() != null)
            {
                throw unreadable.get();
            }
            return rotated.get();
        }, OLD_KEK, kek ->
        {
            if (kek.equals(UNUSABLE))
            {
                throw new IllegalStateException("no grant");
            }
        }, SCOPE, Duration.ofHours(1));
        watch.look();
        assertThat(watch.get()).isEqualTo(OLD_KEK);

        rotated.set(Optional.of(UNUSABLE));
        watch.look();
        watch.look();
        assertThat(watch.get()).isEqualTo(OLD_KEK);
        assertThat(audit.names().stream().filter("keys.kek_refused"::equals))
                .hasSize(1);
        assertThat(audit.attributes("keys.kek_refused"))
                .containsEntry("sdh.kek", UNUSABLE)
                .containsEntry("sdh.control_scope", SCOPE);

        rotated.set(Optional.of(NEW_KEK));
        watch.look();
        assertThat(watch.get()).isEqualTo(NEW_KEK);
        assertThat(audit.attributes("keys.kek_active"))
                .containsEntry("sdh.kek", NEW_KEK);

        unreadable.set(new IllegalStateException("down"));
        watch.look();
        assertThat(watch.get()).isEqualTo(NEW_KEK);
        watch.close();
    }

    /**
     * Every table of a stage's rows with keys is named as its store makes
     * it: each counts, empty. A failure's and its event's keys are
     * re-wrapped to the new KEK, the same key; another stage's rows are
     * left as they are.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reWrapsAStagesDatabaseRows() throws Exception
    {
        DataSource database = StageTestSupport.database();
        makeStores(database);
        for (WrappedRows each : StageKeys.splitRows(schema))
        {
            assertThat(each.byKek(database, STAGE)).isEmpty();
        }
        byte[] reasonKey = new byte[32];
        new Random(7).nextBytes(reasonKey);
        UUID item = UUID.randomUUID();
        failure(database, STAGE, item, reasonKey);
        failure(database, "transform", item, reasonKey);

        List<WrappedRows> rows = StageKeys.rows(schema);
        long changed = 0;
        for (WrappedRows each : rows)
        {
            changed += each.rekey(database, STAGE, keys, OLD_KEK, NEW_KEK);
        }
        assertThat(changed).isEqualTo(2);
        WrappedRows failures = rows.getFirst();
        assertThat(failures.byKek(database, STAGE)).containsOnlyKeys(NEW_KEK);
        assertThat(failures.byKek(database, "transform"))
                .containsOnlyKeys(OLD_KEK);
        assertThat(keys.unwrap(storedKey(database, STAGE, item)))
                .isEqualTo(reasonKey);
        assertThat(rows.get(1).byKek(database, STAGE))
                .containsEntry(NEW_KEK, 1L);
    }

    /**
     * The maintenance pass carries a re-key out: the table's files a
     * share each run, then the database rows and the ID key; then it ends
     * it, done, audited, and asks for nothing more. Every item reads back
     * the same.
     *
     * @throws Exception if the test fails
     */
    @Test
    void theMaintenancePassCarriesAReKeyOut() throws Exception
    {
        DataSource database = StageTestSupport.database();
        makeStores(database);
        Landing landing = new Landing();
        byte[] first = data(9000);
        byte[] second = data(20000);
        UUID one = landing.land(first);
        UUID two = landing.land(second);
        byte[] idKey = IdKeys.loadOrCreate(landing.table, landing.committer,
                keys, OLD_KEK);
        byte[] reasonKey = new byte[32];
        failure(database, STAGE, UUID.randomUUID(), reasonKey);

        RekeyPass pass = new RekeyPass(state, landing.committer, keys,
                database, STAGE, StageKeys.rows(schema), 1);
        assertThat(pass.due()).isEmpty();
        state.requestRekey("op-1", "olga", OLD_KEK, NEW_KEK);
        int runs = 0;
        while (state.rekeying().orElseThrow().state().active())
        {
            assertThat(++runs).isLessThan(60);
            TableMaintenance.Purge purge = pass.due().orElseThrow();
            pass.ran(purge, List.of(landing.maintain(purge)));
        }
        ControlState.Rekeying done = state.rekeying().orElseThrow();
        assertThat(done.state()).isEqualTo(ControlState.RekeyState.DONE);
        assertThat(done.files()).isPositive();
        assertThat(done.rows()).isPositive();
        assertThat(pass.due()).isEmpty();
        landing.table.refresh();
        assertThat(Rekey.left(landing.table, OLD_KEK)).isZero();
        assertThat(IdKeys.kek(landing.table)).contains(NEW_KEK);
        assertThat(IdKeys.load(landing.table, keys)).isEqualTo(idKey);
        assertThat(StageKeys.rows(schema).getFirst().byKek(database, STAGE))
                .containsOnlyKeys(NEW_KEK);
        assertThat(landing.readBack(one)).isEqualTo(first);
        assertThat(landing.readBack(two)).isEqualTo(second);
        assertThat(audit.attributes("keys.rekeyed"))
                .containsEntry("sdh.operation_id", "op-1")
                .containsEntry("sdh.kek_from", OLD_KEK)
                .containsEntry("sdh.kek_to", NEW_KEK);
    }

    /**
     * A run that keeps failing fails the re-key after the tries allowed,
     * by the failure's class, audited; one that fails fewer times is tried
     * again.
     *
     * @throws Exception if the test fails
     */
    @Test
    void failsAReKeyAfterTheTriesAllowed() throws Exception
    {
        Landing landing = new Landing();
        RekeyPass pass = new RekeyPass(state, landing.committer, keys,
                StageTestSupport.database(), STAGE, List.of(), 1);
        state.requestRekey("op-1", "olga", OLD_KEK, NEW_KEK);
        for (int i = 1; i < RekeyPass.TRIES; i++)
        {
            pass.failed(pass.due().orElseThrow(),
                    new IllegalStateException("store down"));
        }
        assertThat(state.rekeying().orElseThrow().state().active()).isTrue();
        pass.failed(pass.due().orElseThrow(),
                new IllegalStateException("store down"));
        ControlState.Rekeying failed = state.rekeying().orElseThrow();
        assertThat(failed.state()).isEqualTo(ControlState.RekeyState.FAILED);
        assertThat(failed.failure()).isEqualTo("IllegalStateException");
        assertThat(audit.attributes("keys.rekey_failed"))
                .containsEntry("sdh.operation_id", "op-1");
        assertThat(pass.due()).isEmpty();
    }

    /**
     * {@code rotate} checks the KEK first, then rotates, once; {@code keys}
     * shows the KEK in use and the rows under each; {@code rekey} needs a
     * second person, is refused from the KEK in use, and asks for it,
     * returning at once with {@code --wait 0}. The API serves all three.
     *
     * @throws Exception if the test fails
     */
    @Test
    void theCommandsRotateCountAndAskForAReKey() throws Exception
    {
        DataSource database = StageTestSupport.database();
        makeStores(database);
        failure(database, STAGE, UUID.randomUUID(), new byte[32]);
        Context context = new Context();

        assertThatThrownBy(() -> run(context, "rotate", "--kek", UNUSABLE))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("other-keks");
        assertThat(state.kek()).isEmpty();
        assertThatThrownBy(() -> run(context, "rotate", "--kek", "bad kek"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThat(run(context, "rotate", "--kek", NEW_KEK))
                .contains("Rotated " + SCOPE);
        assertThat(state.kek().orElseThrow().setBy()).isEqualTo("alice");
        assertThat(run(context, "rotate", "--kek", NEW_KEK))
                .contains("already wraps new data with " + NEW_KEK);

        assertThat(run(context, "keys")).contains("wraps new data with "
                + NEW_KEK).contains(OLD_KEK + " 1");

        assertThatThrownBy(() -> run(context, "rekey", "--from", NEW_KEK,
                "--to", OLD_KEK)).isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("rotate first");
        assertThatThrownBy(() -> run(context, "rekey", "--from", OLD_KEK,
                "--to", OLD_KEK)).isInstanceOf(AdminArgs.UsageException.class);
        assertThat(run(context, "rekey", "--from", OLD_KEK, "--to", NEW_KEK,
                "--wait", "0")).contains("requested").contains("Still");
        assertThat(state.rekeying().orElseThrow().requestedBy())
                .isEqualTo("alice");
        assertThat(approval(command("rekey")))
                .isEqualTo(Operation.Approval.FOUR_EYES);
        assertThat(approval(command("rotate")))
                .isEqualTo(Operation.Approval.NONE);
        assertThat(ControlRoute.all().stream().map(ControlRoute::key))
                .anyMatch(key -> key.endsWith("/keys"))
                .anyMatch(key -> key.endsWith("/keys/rotate"))
                .anyMatch(key -> key.endsWith("/keys/rekey"));
    }

    /**
     * Makes every store's tables in the schema.
     *
     * @param database the database
     */
    void makeStores(DataSource database)
    {
        Duration wait = Duration.ofSeconds(10);
        new FailureStore(database, schema, STAGE, wait).createTableIfMissing();
        new CorrectionStore(database, schema, STAGE, wait)
                .createTableIfMissing();
        new ReprocessStore(database, schema, STAGE, wait)
                .createTablesIfMissing();
        new StageReleases(database, schema, STAGE, wait)
                .createTablesIfMissing();
        new HoldReleases(database, schema, wait).createTableIfMissing();
    }

    /**
     * Stores a failure of a stage, and an event of it, each with a reason
     * key wrapped with the old KEK.
     *
     * @param database the database
     * @param stage    the stage
     * @param item     the item
     * @param key      the reason's key
     * @throws Exception if it can't be stored
     */
    void failure(DataSource database, String stage, UUID item, byte[] key)
            throws Exception
    {
        WrappedKey wrapped = keys.wrap(OLD_KEK, key);
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection connection = database.getConnection();
                PreparedStatement failure = connection.prepareStatement(
                        "INSERT INTO " + FailureStore.table(schema)
                                + " (stage, item_id, seq, error_code,"
                                + " source_offset, failed_at, state,"
                                + " changed_at, reason_key, reason_kek)"
                                + " VALUES (?, ?, 1, 'E', 0, ?, 'open', ?,"
                                + " ?, ?)");
                PreparedStatement event = connection.prepareStatement(
                        "INSERT INTO " + FailureStore.eventsTable(schema)
                                + " (stage, item_id, seq, event, at,"
                                + " operator, from_state, to_state,"
                                + " reason_key, reason_kek) VALUES (?, ?,"
                                + " 1, 1, ?, 'olga', 'open', 'declined', ?,"
                                + " ?)"))
        {
            failure.setString(1, stage);
            failure.setObject(2, item);
            failure.setTimestamp(3, now);
            failure.setTimestamp(4, now);
            failure.setBytes(5, wrapped.wrapped());
            failure.setString(6, OLD_KEK);
            failure.executeUpdate();
            event.setString(1, stage);
            event.setObject(2, item);
            event.setTimestamp(3, now);
            event.setBytes(4, wrapped.wrapped());
            event.setString(5, OLD_KEK);
            event.executeUpdate();
        }
    }

    /**
     * A failure's stored reason key.
     *
     * @param database the database
     * @param stage    its stage
     * @param item     its item
     * @return the key as stored
     * @throws Exception if it can't be read
     */
    WrappedKey storedKey(DataSource database, String stage, UUID item)
            throws Exception
    {
        try (Connection connection = database.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT reason_kek, reason_key FROM "
                                + FailureStore.table(schema)
                                + " WHERE stage = ? AND item_id = ?"))
        {
            select.setString(1, stage);
            select.setObject(2, item);
            try (ResultSet rows = select.executeQuery())
            {
                assertThat(rows.next()).isTrue();
                return new WrappedKey(rows.getString(1), rows.getBytes(2));
            }
        }
    }

    /**
     * Random bytes, the same each time for the same length.
     *
     * @param length how many
     * @return the bytes
     */
    static byte[] data(int length)
    {
        byte[] bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }

    /**
     * Runs a key command as {@code alice}.
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
     * A key command.
     *
     * @param name its name
     * @return it
     */
    static AdminCommand<Context> command(String name)
    {
        return KeyCommands.<Context>all().stream()
                .filter(each -> each.name().equals(name)).findFirst()
                .orElseThrow();
    }

    /**
     * The approval a command's requests need.
     *
     * @param command the command
     * @return it
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static Operation.Approval approval(AdminCommand<Context> command)
    {
        return ((Operation) command).approval(null);
    }

    /** The stage, as the key commands see it: no tables of its own. */
    final class Context implements KeyAdmin
    {

        /** Creates the context. */
        Context()
        {
        }

        @Override
        public ControlState controlState(boolean change)
        {
            return state;
        }

        @Override
        public String settingKek()
        {
            return OLD_KEK;
        }

        @Override
        public KeyProvider keyProvider()
        {
            return keys;
        }

        @Override
        public List<Table> keyTables()
        {
            return List.of();
        }

        @Override
        public List<WrappedRows> wrappedRows()
        {
            return StageKeys.rows(schema);
        }

        @Override
        public DataSource keyDatabase()
        {
            return StageTestSupport.database();
        }

        @Override
        public String stage()
        {
            return STAGE;
        }
    }

    /** A {@code landed_items} table, its committer and lander. */
    final class Landing
    {

        /** The intake. */
        static final String INTAKE = "claims-sftp";

        /** Makes the digests and MACs, with an all-zero ID key. */
        final RecordIds ids = new RecordIds(new byte[32]);

        /** The table. */
        final Table table;

        /** Commits under one lease epoch. */
        final FencedCommitter committer;

        /** Creates the catalog and the table. */
        Landing()
        {
            InMemoryCatalog catalog = new InMemoryCatalog();
            catalog.initialize("prod", Map.of(
                    CatalogProperties.WAREHOUSE_LOCATION,
                    "memory://wh-" + UUID.randomUUID()));
            table = LandingTables.landedItems(catalog,
                    LandingTables.DEFAULT_NAMESPACE);
            committer = new FencedCommitter(new LeaseToken()
            {
                @Override
                public long epoch()
                {
                    return 1;
                }

                @Override
                public void checkHeld()
                {
                }
            });
            committer.establish(table);
        }

        /**
         * Lands an item with the old KEK, a commit every few chunks.
         *
         * @param bytes the item
         * @return its ID
         * @throws Exception if it can't be landed
         */
        UUID land(byte[] bytes) throws Exception
        {
            ItemLander lander = new ItemLander(table, committer, keys, ids,
                    ItemLander.Settings.defaults(INTAKE, OLD_KEK)
                            .withSizes(1024, 4096, 4096),
                    Clock.systemUTC());
            String name = "item-" + bytes.length;
            NewItem item = new NewItem(UuidV7.next(
                    System.currentTimeMillis()), ItemKind.FILE,
                    SourceRef.of(ids, INTAKE, name, bytes.length, "1"), name,
                    null);
            return lander.land(item, offset -> new ByteArrayInputStream(
                    bytes, (int) offset, bytes.length - (int) offset))
                    .itemId();
        }

        /**
         * Runs maintenance with a purge.
         *
         * @param purge the purge
         * @return what it did
         */
        TableMaintenance.Report maintain(TableMaintenance.Purge purge)
        {
            return new TableMaintenance(committer, new MaintenanceSettings(
                    Duration.ofDays(7), 1, false, 0.75, 2, 8L << 30,
                    Duration.ofDays(3)), Clock.fixed(Instant.now(),
                            ZoneOffset.UTC))
                    .run(table, LandingTables.LANDED_AT, Optional.empty(),
                            false, UnaryOperator.identity(), purge);
        }

        /**
         * Reads a landed item back.
         *
         * @param itemId the item
         * @return its bytes
         * @throws Exception if it can't be read
         */
        byte[] readBack(UUID itemId) throws Exception
        {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            LandedItems.read(table, itemId, keys, ids, output);
            return output.toByteArray();
        }
    }
}
