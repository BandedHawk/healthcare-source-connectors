/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
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
import com.sdh.healthcare.connector.core.maintenance.ItemPurge;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.TableMaintenance;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.DataAdmin;
import com.sdh.healthcare.connector.stage.admin.DataCommands;
import com.sdh.healthcare.connector.stage.admin.Operation;
import com.sdh.healthcare.connector.stage.control.ControlRoute;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import javax.crypto.KeyGenerator;
import javax.sql.DataSource;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A stage's retention, item purges and erasure (CONTROL-API-DESIGN.md §11,
 * HIPAA.md P4): the state kept, the policy maintenance applies, the
 * maintenance pass that carries a purge out on {@code landed_items}, and
 * the commands, an erase's certificate checked with the stage's key.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class DataLifecycleTest
{

    /** The stage's KEK. */
    static final String KEK = "records-kek/1";

    /** The stage. */
    static final String STAGE = "convert";

    /** The scope. */
    static final String SCOPE = "convert/test";

    /** The intake items are landed from. */
    static final String INTAKE = "claims-sftp";

    /** Captures each test's audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Holds the KEK. */
    AesKeyWrapProvider keys;

    /** The schema of this test's tables. */
    String schema;

    /** The scope's state. */
    ControlState state;

    /** The scope's purges. */
    PurgeRequests purges;

    /** Creates the tests; JUnit runs each test on a new instance. */
    DataLifecycleTest()
    {
    }

    /**
     * Makes the KEK, and a schema of the test's own.
     *
     * @throws Exception if the KEK can't be made
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
    }

    /**
     * No retention is set until it is; forever is one; none of zero is
     * taken. An erasure keeps the first operation's, and the scope then
     * refuses to start; another scope is its own.
     */
    @Test
    void keepsTheRetentionSetAndTheErasure()
    {
        assertThat(state.retention()).isEmpty();
        assertThat(state.erased()).isEmpty();
        state.refuseIfErased();

        ControlState.Retention set = state.setRetention("olga",
                Optional.of(Duration.ofDays(30)));
        assertThat(set.keep()).contains(Duration.ofDays(30));
        assertThat(set.wire()).isEqualTo("PT720H");
        assertThat(state.retention().orElseThrow().setBy()).isEqualTo("olga");
        assertThat(state.setRetention("pat", Optional.empty()).wire())
                .isEqualTo(ControlState.FOREVER);
        assertThat(state.retention().orElseThrow().keep()).isEmpty();
        assertThatThrownBy(() -> state.setRetention("olga",
                Optional.of(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);

        ControlState.Erased erased = state.erase("op-1", "olga");
        assertThat(erased.id()).isEqualTo("op-1");
        assertThat(state.erase("op-2", "pat").id()).isEqualTo("op-1");
        assertThatThrownBy(state::refuseIfErased)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("erased");
        new ControlState(StageTestSupport.database(), schema, "split/test")
                .refuseIfErased();
    }

    /**
     * The policy takes the bounds' minimum over the stage's floor, the
     * operator's retention over the setting's, never less than the
     * minimum, and keeps every row when the state can't be read. Asked
     * for, a retention outside the bounds is refused, as is forever under
     * a maximum.
     */
    @Test
    void thePolicyKeepsWithinItsBounds()
    {
        RetentionPolicy policy = RetentionPolicy.of(Duration.ofDays(30),
                Duration.ofDays(1), new CommonSettings.RetentionBounds(
                        Duration.ofDays(7), Duration.ofDays(365)));
        assertThat(policy.minimum()).isEqualTo(Duration.ofDays(7));
        assertThat(policy.inForce(state)).contains(Duration.ofDays(30));
        state.setRetention("olga", Optional.of(Duration.ofDays(90)));
        assertThat(policy.forRuns(state).get()).contains(Duration.ofDays(90));
        state.setRetention("olga", Optional.of(Duration.ofDays(2)));
        assertThat(policy.inForce(state)).contains(Duration.ofDays(7));

        assertThatThrownBy(() -> policy.check(Optional.of(Duration.ofDays(
                3)))).hasMessageContaining("shortest");
        assertThatThrownBy(() -> policy.check(Optional.of(Duration.ofDays(
                400)))).hasMessageContaining("longest");
        assertThatThrownBy(() -> policy.check(Optional.empty()))
                .hasMessageContaining("forever");
        assertThat(RetentionPolicy.of(null, Duration.ofDays(1), null)
                .check(Optional.empty())).isEmpty();

        ControlState down = new ControlState(unreachable(), schema, SCOPE);
        assertThat(policy.forRuns(down).get()).isEmpty();
        assertThatThrownBy(() -> new CommonSettings.RetentionBounds(
                Duration.ofDays(9), Duration.ofDays(8)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * A purge asked for is kept with who, when and the ticket; asked again
     * by its operation, it is the same, but not for another item. The
     * pass then rewrites the files holding the item, one a run within a
     * one-byte budget, until a run is quiet; then the item has no row, its
     * correction is gone, its failure is kept, the other items read back
     * the same, the files replaced are gone, and the purge is done and
     * audited.
     *
     * @throws Exception if the test fails
     */
    @Test
    void theMaintenancePassPurgesAnItem() throws Exception
    {
        DataSource database = StageTestSupport.database();
        makeStores(database);
        Landing landing = new Landing();
        byte[] first = data(9000);
        byte[] second = data(20000);
        byte[] third = data(5000);
        UUID one = landing.land(first);
        UUID two = landing.land(second);
        UUID three = landing.land(third);
        correction(database, two);
        failure(database, two);
        landing.table.refresh();
        assertThat(ItemPurge.left(landing.table, Set.of(two))).containsKey(
                two);

        PurgePass pass = new PurgePass(purges, landing.committer, database,
                CorrectionStore.table(schema), STAGE, 1);
        assertThat(pass.due()).isEmpty();
        PurgeRequests.Purge asked = purges.request("op-1", "olga", two,
                "CHG-7");
        assertThat(asked.state()).isEqualTo(PurgeRequests.State.REQUESTED);
        assertThat(purges.request("op-1", "olga", two, "CHG-7"))
                .isEqualTo(asked);
        assertThatThrownBy(() -> purges.request("op-1", "olga", one,
                "CHG-7")).isInstanceOf(IllegalArgumentException.class);
        int runs = 0;
        while (purges.get("op-1").orElseThrow().state().active())
        {
            assertThat(++runs).isLessThan(60);
            TableMaintenance.Purge purge = pass.due().orElseThrow();
            pass.ran(purge, List.of(landing.maintain(purge)));
        }
        PurgeRequests.Purge done = purges.get("op-1").orElseThrow();
        assertThat(done.state()).isEqualTo(PurgeRequests.State.DONE);
        assertThat(done.files()).isPositive();
        assertThat(done.rows()).isPositive();
        assertThat(runs).isGreaterThan(2);
        assertThat(pass.due()).isEmpty();
        landing.table.refresh();
        assertThat(ItemPurge.left(landing.table, Set.of(one, two, three)))
                .containsOnlyKeys(one, three);
        assertThat(landing.readBack(one)).isEqualTo(first);
        assertThat(landing.readBack(three)).isEqualTo(third);
        assertThat(count(database, CorrectionStore.table(schema), two))
                .isZero();
        assertThat(count(database, FailureStore.table(schema), two))
                .isEqualTo(1);
        assertThat(audit.attributes("data.purged"))
                .containsEntry("sdh.operation_id", "op-1")
                .containsEntry("sdh.item_id", two.toString())
                .containsEntry("sdh.ticket", "CHG-7")
                .containsEntry("sdh.count", 1L);
        assertThat(purges.recent(5)).extracting(PurgeRequests.Purge::id)
                .containsExactly("op-1");
    }

    /**
     * A run that keeps failing fails the purges in it after the tries
     * allowed, by the failure's class, audited.
     */
    @Test
    void failsAPurgeAfterTheTriesAllowed()
    {
        Landing landing = new Landing();
        PurgePass pass = new PurgePass(purges, landing.committer,
                StageTestSupport.database(), null, STAGE, 1);
        purges.request("op-1", "olga", UUID.randomUUID(), "CHG-7");
        for (int i = 1; i < PurgePass.TRIES; i++)
        {
            pass.failed(pass.due().orElseThrow(),
                    new IllegalStateException("store down"));
        }
        assertThat(purges.get("op-1").orElseThrow().state().active())
                .isTrue();
        pass.failed(pass.due().orElseThrow(),
                new IllegalStateException("store down"));
        PurgeRequests.Purge failed = purges.get("op-1").orElseThrow();
        assertThat(failed.state()).isEqualTo(PurgeRequests.State.FAILED);
        assertThat(failed.failure()).isEqualTo("IllegalStateException");
        assertThat(audit.attributes("data.purge_failed"))
                .containsEntry("sdh.operation_id", "op-1");
        assertThat(pass.due()).isEmpty();
    }

    /**
     * {@code retention-set} is refused outside the bounds and sets one
     * within them, which {@code retention} shows; {@code purge} needs a
     * ticket and a second person, and asks for it, returning at once with
     * {@code --wait 0}, and {@code purges} lists it. The API serves them
     * all.
     *
     * @throws Exception if the test fails
     */
    @Test
    void theCommandsSetTheRetentionAndAskForAPurge() throws Exception
    {
        Context context = new Context(new Landing(), false);
        assertThat(run(context, "retention")).contains("keeps rows 30"
                + " day(s), its setting's");
        assertThatThrownBy(() -> run(context, "retention-set", "--days",
                "3")).isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("shortest");
        assertThatThrownBy(() -> run(context, "retention-set", "--forever"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run(context, "retention-set"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run(context, "retention-set", "--days",
                "x")).isInstanceOf(AdminArgs.UsageException.class);
        assertThat(run(context, "retention-set", "--days", "90"))
                .contains("keeps rows 90 day(s), set by alice");
        assertThat(state.retention().orElseThrow().keep())
                .contains(Duration.ofDays(90));

        UUID item = UUID.randomUUID();
        assertThatThrownBy(() -> run(context, "purge", "--item-id",
                item.toString())).isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("ticket");
        assertThatThrownBy(() -> run(context, "purge", "--item-id",
                item.toString(), "--ticket", "free text here"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run(context, "purge", "--item-id", "nope",
                "--ticket", "CHG-7"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThat(run(context, "purge", "--item-id", item.toString(),
                "--ticket", "CHG-7", "--wait", "0")).contains("requested")
                .contains("Still").contains("receiver's to purge");
        assertThat(purges.active()).singleElement()
                .satisfies(each -> assertThat(each.itemId()).isEqualTo(item));
        assertThat(run(context, "purges")).contains("of item " + item)
                .contains("CHG-7");

        assertThat(approval("purge")).isEqualTo(Operation.Approval.FOUR_EYES);
        assertThat(approval("erase")).isEqualTo(Operation.Approval.FOUR_EYES);
        assertThat(approval("retention-set"))
                .isEqualTo(Operation.Approval.NONE);
        assertThat(ControlRoute.all().stream().map(ControlRoute::key))
                .contains("GET /api/v1/retention", "PUT /api/v1/retention",
                        "GET /api/v1/purges",
                        "POST /api/v1/items/{itemId}/purge",
                        "POST /api/v1/erase");
    }

    /**
     * {@code erase} is refused until the stage is paused, and for a stage
     * that drains, until its pause has settled. Then it marks the scope
     * erased, drops its tables, deletes its rows and not another stage's,
     * and answers a certificate signed with the stage's key, naming the
     * tables, the KEKs and the journal's heads.
     *
     * @throws Exception if the test fails
     */
    @Test
    void eraseDropsEverythingAndSignsACertificate() throws Exception
    {
        DataSource database = StageTestSupport.database();
        makeStores(database);
        Landing landing = new Landing();
        landing.land(data(9000));
        failure(database, UUID.randomUUID());
        failureOf(database, "transform", UUID.randomUUID());
        AuditTables journal = new AuditTables(schema);
        journal.createIfMissing(database, 10);
        UUID chain = UUID.randomUUID();
        try (Connection connection = database.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute("INSERT INTO " + journal.chains()
                    + " VALUES ('" + chain + "', '" + SCOPE + "', 'pod-1',"
                    + " now(), 42, '\\x0a0b'::bytea, now(), NULL)");
        }

        assertThatThrownBy(() -> run(new Context(landing, false), "erase",
                "--ticket", "CHG-9"))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("isn't paused");
        state.pause("olga", "contract end");
        assertThatThrownBy(() -> run(new Context(landing, true), "erase",
                "--ticket", "CHG-9"))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("too lately");
        assertThat(state.erased()).isEmpty();

        Context context = new Context(landing, false);
        String printed = run(context, "erase", "--ticket", "CHG-9");
        assertThat(printed).contains("Erased " + SCOPE + ": 1 table(s)")
                .contains(KEK);
        assertThat(landing.catalog.tableExists(landing.id)).isFalse();
        assertThat(state.erased().orElseThrow().erasedBy())
                .isEqualTo("alice");
        assertThatThrownBy(state::refuseIfErased)
                .isInstanceOf(IllegalStateException.class);
        assertThat(countOf(database, FailureStore.table(schema), STAGE))
                .isZero();
        assertThat(countOf(database, FailureStore.table(schema),
                "transform")).isEqualTo(1);

        String certificate = printed.strip().lines().reduce((a, b) -> b)
                .orElseThrow();
        JWSObject jws = JWSObject.parse(certificate);
        assertThat(jws.verify(new ECDSAVerifier(
                (ECPublicKey) context.signing.getPublic()))).isTrue();
        assertThat(jws.getHeader().getType().getType())
                .isEqualTo(DataCommands.CERTIFICATE_TYPE);
        assertThat(jws.getHeader().getKeyID()).isEqualTo("stage-key");
        Map<String, Object> claims = jws.getPayload().toJSONObject();
        assertThat(claims).containsEntry("scope", SCOPE)
                .containsEntry("ticket", "CHG-9")
                .containsEntry("requestedBy", "alice")
                .containsEntry("signingKid", "stage-key");
        assertThat(claims.get("keks")).isEqualTo(List.of(KEK));
        assertThat((List<?>) claims.get("tables")).singleElement()
                .satisfies(table -> assertThat(((Map<?, ?>) table)
                        .get("name")).isEqualTo(landing.id.toString()));
        assertThat((List<?>) claims.get("journalHeads")).singleElement()
                .satisfies(head ->
                {
                    assertThat(((Map<?, ?>) head).get("chainId"))
                            .isEqualTo(chain.toString());
                    assertThat(((Map<?, ?>) head).get("hash"))
                            .isEqualTo("0a0b");
                });
    }

    /**
     * Makes the stores' tables in the schema.
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
    }

    /**
     * Stores a failure of the stage's.
     *
     * @param database the database
     * @param item     the item
     * @throws Exception if it can't be stored
     */
    void failure(DataSource database, UUID item) throws Exception
    {
        failureOf(database, STAGE, item);
    }

    /**
     * Stores a failure, with a reason key wrapped with the KEK.
     *
     * @param database the database
     * @param stage    its stage
     * @param item     the item
     * @throws Exception if it can't be stored
     */
    void failureOf(DataSource database, String stage, UUID item)
            throws Exception
    {
        WrappedKey wrapped = keys.wrap(KEK, new byte[32]);
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection connection = database.getConnection();
                PreparedStatement failure = connection.prepareStatement(
                        "INSERT INTO " + FailureStore.table(schema)
                                + " (stage, item_id, seq, error_code,"
                                + " source_offset, failed_at, state,"
                                + " changed_at, reason_key, reason_kek)"
                                + " VALUES (?, ?, 1, 'E', 0, ?, 'open', ?,"
                                + " ?, ?)"))
        {
            failure.setString(1, stage);
            failure.setObject(2, item);
            failure.setTimestamp(3, now);
            failure.setTimestamp(4, now);
            failure.setBytes(5, wrapped.wrapped());
            failure.setString(6, KEK);
            failure.executeUpdate();
        }
    }

    /**
     * Stores a correction of the stage's.
     *
     * @param database the database
     * @param item     the item
     * @throws Exception if it can't be stored
     */
    void correction(DataSource database, UUID item) throws Exception
    {
        WrappedKey wrapped = keys.wrap(KEK, new byte[32]);
        try (Connection connection = database.getConnection();
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + CorrectionStore.table(schema)
                                + " (stage, item_id, seq, correction, state,"
                                + " submitted_at, submitted_by, content_enc,"
                                + " content_key, content_kek,"
                                + " content_length) VALUES (?, ?, 1, 1,"
                                + " 'pending', now(), 'olga', ?, ?, ?, 3)"))
        {
            insert.setString(1, STAGE);
            insert.setObject(2, item);
            insert.setBytes(3, new byte[] {1, 2, 3});
            insert.setBytes(4, wrapped.wrapped());
            insert.setString(5, KEK);
            insert.executeUpdate();
        }
    }

    /**
     * The stage's rows of an item in a table.
     *
     * @param database the database
     * @param table    the table
     * @param item     the item
     * @return how many
     * @throws Exception if it can't be read
     */
    static long count(DataSource database, String table, UUID item)
            throws Exception
    {
        try (Connection connection = database.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT count(*) FROM " + table
                                + " WHERE stage = ? AND item_id = ?"))
        {
            select.setString(1, STAGE);
            select.setObject(2, item);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    /**
     * A stage's rows in a table.
     *
     * @param database the database
     * @param table    the table
     * @param stage    the stage
     * @return how many
     * @throws Exception if it can't be read
     */
    static long countOf(DataSource database, String table, String stage)
            throws Exception
    {
        try (Connection connection = database.getConnection();
                PreparedStatement select = connection.prepareStatement(
                        "SELECT count(*) FROM " + table
                                + " WHERE stage = ?"))
        {
            select.setString(1, stage);
            try (ResultSet rows = select.executeQuery())
            {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    /**
     * A database that can't be reached.
     *
     * @return it
     */
    static DataSource unreachable()
    {
        return (DataSource) Proxy.newProxyInstance(
                DataLifecycleTest.class.getClassLoader(),
                new Class<?>[] {DataSource.class}, (proxy, method, args) ->
                {
                    throw new SQLException("down", "08001");
                });
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
     * Runs a data command as {@code alice}.
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
     * A data command.
     *
     * @param name its name
     * @return it
     */
    static AdminCommand<Context> command(String name)
    {
        return DataCommands.<Context>all().stream()
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

    /** The stage, as the data commands see it: one landing table. */
    final class Context implements DataAdmin
    {

        /** The stage's table. */
        final Landing landing;

        /** Whether it drains. */
        final boolean drains;

        /** The stage's signing key. */
        final KeyPair signing;

        /**
         * Creates the context.
         *
         * @param landing the stage's table
         * @param drains  whether it drains
         * @throws Exception if the signing key can't be made
         */
        Context(Landing landing, boolean drains) throws Exception
        {
            this.landing = landing;
            this.drains = drains;
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
            return keys;
        }

        @Override
        public List<Table> keyTables()
        {
            return landing.catalog.tableExists(landing.id)
                    ? List.of(landing.catalog.loadTable(landing.id))
                    : List.of();
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
            return List.of(new DataAdmin.Erasable(landing.catalog,
                    landing.id));
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
        public boolean drains()
        {
            return drains;
        }
    }

    /** A landing table in a catalog of its own. */
    final class Landing
    {

        /** Makes the digests and MACs, with an all-zero ID key. */
        final RecordIds ids = new RecordIds(new byte[32]);

        /** The catalog. */
        final InMemoryCatalog catalog = new InMemoryCatalog();

        /** The table's name. */
        final TableIdentifier id = TableIdentifier.of(
                LandingTables.DEFAULT_NAMESPACE, LandingTables.LANDED_ITEMS);

        /** The table. */
        final Table table;

        /** Commits under one lease epoch. */
        final FencedCommitter committer;

        /** Creates the catalog and the table. */
        Landing()
        {
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
         * Lands an item, a commit every few chunks.
         *
         * @param bytes the item
         * @return its ID
         * @throws Exception if it can't be landed
         */
        UUID land(byte[] bytes) throws Exception
        {
            ItemLander lander = new ItemLander(table, committer, keys, ids,
                    ItemLander.Settings.defaults(INTAKE, KEK)
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
