/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.telemetry.AuditJournal;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JdbcAuditJournal} and {@link AuditTrail}: what is
 * appended is written, chained and sealed, and any change to it afterwards
 * is found.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class JdbcAuditJournalTest
{

    /** The stage key's ID. */
    private static final String KID = "stage-2026";

    /** How long an unclosed chain may go unwritten in these tests. */
    private static final Duration STALE = Duration.ofMinutes(10);

    /** The database. */
    private final DataSource dataSource = TestPostgres.dataSource();

    /** A schema of each test's own. */
    private AuditTables tables;

    /** The stage's signing key. */
    private KeyPair stageKey;

    /** Creates the tests; JUnit runs each test on a new instance. */
    JdbcAuditJournalTest()
    {
    }

    /**
     * Picks a fresh schema and makes a stage key.
     *
     * @throws Exception if the key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        tables = new AuditTables("audit_"
                + UUID.randomUUID().toString().replace("-", "")
                        .substring(0, 12));
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        stageKey = generator.generateKeyPair();
    }

    /**
     * Everything appended is written in order, sealed at the end, and
     * verifies; the export returns it.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void writesChainsAndSealsWhatIsAppended() throws Exception
    {
        UUID chain = journal(250, true);

        AuditTrail.Verification verification = trail().verify(null, null,
                key(stageKey), Instant.now(), STALE);

        assertThat(verification.intact()).isTrue();
        assertThat(verification.chains()).singleElement().satisfies(report ->
        {
            assertThat(report.chainId()).isEqualTo(chain);
            assertThat(report.entries()).isEqualTo(250);
            assertThat(report.sealedUpTo()).isEqualTo(250);
            assertThat(report.closedAt()).isNotNull();
            assertThat(report.problems()).isEmpty();
        });
        List<AuditTrail.Exported> exported = new ArrayList<>();
        assertThat(trail().export(null, null, exported::add)).isEqualTo(250);
        assertThat(exported.getFirst().name()).isEqualTo("sdh.audit.test");
        assertThat(exported.getFirst().attributes())
                .containsEntry("sdh.seq", "0");
    }

    /**
     * An entry changed after it was written breaks the chain there.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void findsAChangedEntry() throws Exception
    {
        journal(20, true);
        sql("UPDATE " + tables.events() + " SET attributes ="
                + " '{\"sdh.seq\":\"999\"}' WHERE seq = 7");

        AuditTrail.Verification verification = verify();

        assertThat(verification.intact()).isFalse();
        assertThat(verification.chains().getFirst().problems())
                .contains(new AuditTrail.Problem(AuditTrail.Kind.BROKEN, 7,
                        1));
    }

    /**
     * Entries deleted from the middle leave a gap, and the next entry no
     * longer follows.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void findsDeletedEntries() throws Exception
    {
        journal(20, true);
        sql("DELETE FROM " + tables.events() + " WHERE seq IN (5, 6)");

        List<AuditTrail.Problem> problems =
                verify().chains().getFirst().problems();

        assertThat(problems).contains(
                new AuditTrail.Problem(AuditTrail.Kind.GAP, 5, 2),
                new AuditTrail.Problem(AuditTrail.Kind.BROKEN, 7, 1));
    }

    /**
     * Entries deleted from the end are found by the chain's head, and the
     * final seal is left naming an entry that isn't there.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void findsATruncatedChain() throws Exception
    {
        journal(20, true);
        sql("DELETE FROM " + tables.events() + " WHERE seq > 15");

        List<AuditTrail.Problem> problems =
                verify().chains().getFirst().problems();

        assertThat(problems).contains(
                new AuditTrail.Problem(AuditTrail.Kind.HEAD_MISSING, 16, 5),
                new AuditTrail.Problem(AuditTrail.Kind.SEAL_ORPHAN, 20, 1));
    }

    /**
     * Rewriting the whole chain, head and hashes, still fails: the seal is
     * over the old hash, and can't be made again without the stage's key.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void findsARewrittenChainByItsSeal() throws Exception
    {
        UUID chain = journal(3, true);
        // Recompute every hash after changing the second entry.
        byte[] previous = AuditEntry.GENESIS;
        List<AuditTrail.Exported> rows = new ArrayList<>();
        trail().export(null, null, rows::add);
        try (Connection connection = dataSource.getConnection())
        {
            for (AuditTrail.Exported row : rows)
            {
                TreeMap<String, String> attributes =
                        new TreeMap<>(row.attributes());
                if (row.seq() == 2)
                {
                    attributes.put("sdh.seq", "999");
                }
                AuditEntry rewritten = AuditEntry.next(chain, row.seq(),
                        row.at(), row.name(), row.severity(), attributes,
                        previous);
                try (var update = connection.prepareStatement("UPDATE "
                        + tables.events() + " SET attributes = ?, hash = ?"
                        + " WHERE chain_id = ? AND seq = ?"))
                {
                    update.setString(1, rewritten.attributesJson());
                    update.setBytes(2, rewritten.hash());
                    update.setObject(3, chain);
                    update.setLong(4, row.seq());
                    update.executeUpdate();
                }
                previous = rewritten.hash();
            }
            try (var head = connection.prepareStatement("UPDATE "
                    + tables.chains() + " SET last_hash = ?"))
            {
                head.setBytes(1, previous);
                head.executeUpdate();
            }
        }

        List<AuditTrail.Problem> problems =
                verify().chains().getFirst().problems();

        assertThat(problems).containsExactly(
                new AuditTrail.Problem(AuditTrail.Kind.SEAL_MISMATCH, 3, 1),
                new AuditTrail.Problem(AuditTrail.Kind.UNSEALED_TAIL, 1, 3));
    }

    /**
     * Seals checked with another key are invalid; a journal without a
     * sealer leaves its chain unsealed, which is a warning, not tampering.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void checksSealsWithTheStageKeyOnly() throws Exception
    {
        journal(5, true);
        journal(5, false);
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));

        AuditTrail.Verification other = trail().verify(null, null,
                key(generator.generateKeyPair()), Instant.now(), STALE);
        AuditTrail.Verification own = verify();

        assertThat(other.chains().getFirst().problems()).contains(
                new AuditTrail.Problem(AuditTrail.Kind.SEAL_INVALID, 5, 1));
        assertThat(own.intact()).isTrue();
        assertThat(own.chains().get(1).problems()).containsExactly(
                new AuditTrail.Problem(AuditTrail.Kind.UNSEALED_TAIL, 1, 5));
    }

    /**
     * A chain whose process stopped without closing it is reported once it
     * has gone unwritten for a while, as an early end, not tampering.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void reportsAChainLeftOpen() throws Exception
    {
        JdbcAuditJournal journal = newJournal(true);
        append(journal, 3);
        journal.flush();
        // The process dies: the journal is never closed.

        AuditTrail.Verification later = trail().verify(null, null,
                key(stageKey), Instant.now().plus(Duration.ofHours(1)),
                STALE);

        assertThat(later.intact()).isTrue();
        assertThat(later.chains().getFirst().problems())
                .extracting(AuditTrail.Problem::kind)
                .contains(AuditTrail.Kind.UNCLOSED);
    }

    /**
     * Entries written while a chain is being checked, after its head row
     * was read, aren't taken for a change: the check reads one snapshot.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void checksAChainBeingWrittenAsOfOneMoment() throws Exception
    {
        try (JdbcAuditJournal journal = newJournal(true))
        {
            append(journal, 3);
            journal.flush();
            String entries = "FROM " + tables.events();
            DataSource racing = (DataSource) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {DataSource.class},
                    (source, method, arguments) ->
                    {
                        Object result = invoke(method, dataSource, arguments);
                        if (!(result instanceof Connection connection))
                        {
                            return result;
                        }
                        return Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Connection.class},
                                (proxy, call, values) ->
                                {
                                    if (call.getName()
                                            .equals("prepareStatement")
                                            && values[0].toString()
                                                    .contains(entries))
                                    {
                                        append(journal, 2);
                                        journal.flush();
                                    }
                                    return invoke(call, connection, values);
                                });
                    });

            AuditTrail.Verification verification = new AuditTrail(racing,
                    tables, Duration.ofSeconds(10)).verify(null, null,
                            key(stageKey), Instant.now(), STALE);

            assertThat(verification.intact()).isTrue();
            assertThat(verification.chains().getFirst().entries())
                    .isEqualTo(3);
        }
        assertThat(verify().chains().getFirst().entries()).isEqualTo(5);
    }

    /**
     * Calls a method of the object a proxy stands in for.
     *
     * @param method    the method
     * @param target    the object
     * @param arguments its arguments
     * @return what it returned
     * @throws Throwable what it threw
     */
    private static Object invoke(Method method, Object target,
            Object[] arguments) throws Throwable
    {
        try
        {
            return method.invoke(target, arguments);
        }
        catch (InvocationTargetException failure)
        {
            throw failure.getCause();
        }
    }

    /**
     * With a linger, a batch waits for more entries before it is written,
     * and is written as soon as it is full; closing doesn't wait for the
     * linger, and the chain is whole.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void lingersForAFullBatch() throws Exception
    {
        long started;
        try (JdbcAuditJournal journal = newJournal(true,
                Duration.ofSeconds(30)))
        {
            append(journal, 1);
            Thread.sleep(500);
            assertThat(events()).as("one entry, lingering").isZero();

            append(journal, 6);
            journal.flush();
            assertThat(events()).as("a full batch of 7").isEqualTo(7);

            append(journal, 2);
            started = System.nanoTime();
        }
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .as("closing doesn't wait for the linger")
                .isLessThan(Duration.ofSeconds(10));
        assertThat(events()).isEqualTo(9);
        assertThat(verify().intact()).isTrue();
    }

    /**
     * How many events the journal's table holds.
     *
     * @return the count
     * @throws SQLException if it can't be read
     */
    private long events() throws SQLException
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT count(*) FROM " + tables.events()))
        {
            rows.next();
            return rows.getLong(1);
        }
    }

    /**
     * Pruning removes whole chains last written before the time given.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void prunesWholeChains() throws Exception
    {
        journal(4, true);
        journal(6, true);

        AuditTrail.Pruned none = trail().prune(Instant.EPOCH);
        AuditTrail.Pruned all = trail().prune(
                Instant.now().plus(Duration.ofMinutes(1)));

        assertThat(none).isEqualTo(new AuditTrail.Pruned(0, 0));
        assertThat(all).isEqualTo(new AuditTrail.Pruned(2, 10));
        assertThat(verify().chains()).isEmpty();
    }

    /**
     * Writes a chain of events and closes it.
     *
     * @param events how many
     * @param sealed whether the stage's key seals it
     * @return the chain
     */
    private UUID journal(int events, boolean sealed)
    {
        try (JdbcAuditJournal journal = newJournal(sealed))
        {
            append(journal, events);
            return journal.chainId();
        }
    }

    /**
     * Starts a journal, with small batches.
     *
     * @param sealed whether the stage's key seals it
     * @return the journal
     */
    private JdbcAuditJournal newJournal(boolean sealed)
    {
        return newJournal(sealed, Duration.ZERO);
    }

    /**
     * Starts a journal, with small batches and a linger.
     *
     * @param sealed whether the stage's key seals it
     * @param linger how long a batch waits for more entries
     * @return the journal
     */
    private JdbcAuditJournal newJournal(boolean sealed, Duration linger)
    {
        AuditSeal sealer;
        try
        {
            sealer = sealed ? new AuditSeal(new ECDSASigner(
                    (ECPrivateKey) stageKey.getPrivate()), KID) : null;
        }
        catch (Exception failure)
        {
            throw new IllegalStateException(failure);
        }
        return new JdbcAuditJournal(dataSource, tables, "test/one", "pod-1",
                sealer, new JdbcAuditJournal.Settings(100, 7,
                        Duration.ofHours(1), Duration.ofSeconds(10),
                        Duration.ofSeconds(30), linger),
                Clock.systemUTC()).start();
    }

    /**
     * Appends numbered events.
     *
     * @param journal the journal
     * @param events  how many
     */
    private static void append(JdbcAuditJournal journal, int events)
    {
        for (int number = 0; number < events; number++)
        {
            TreeMap<String, String> attributes = new TreeMap<>();
            attributes.put("sdh.seq", Integer.toString(number));
            journal.append(new AuditJournal.Event(Instant.now(),
                    "sdh.audit.test", "INFO", attributes));
        }
    }

    /**
     * Verifies the journal with the stage's key.
     *
     * @return what was found
     * @throws SQLException if it can't be read
     */
    private AuditTrail.Verification verify() throws SQLException
    {
        return trail().verify(null, null, key(stageKey), Instant.now(),
                STALE);
    }

    /**
     * The journal's reader.
     *
     * @return it
     */
    private AuditTrail trail()
    {
        return new AuditTrail(dataSource, tables, Duration.ofSeconds(10));
    }

    /**
     * A key to check seals with.
     *
     * @param pair the key pair
     * @return its public half, under the stage key's ID
     */
    private static AuditTrail.SealKey key(KeyPair pair)
    {
        return new AuditTrail.SealKey(KID, (ECPublicKey) pair.getPublic());
    }

    /**
     * Runs a statement.
     *
     * @param text the SQL
     * @throws SQLException if it fails
     */
    private void sql(String text) throws SQLException
    {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute(text);
        }
    }
}
