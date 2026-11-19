/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.StuckCommand;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * {@code stuck} (STAGES.md §13.6) against a registry in embedded Postgres:
 * which items it lists, why each waits, and the claims by pod.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StuckCommandTest
{

    /** Captures the audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The registry's schema. */
    String schema;

    /** 1 of 3 stored, 1 left claimed past its claim, 1 not received. */
    final UUID expired = UUID.randomUUID();

    /** 0 of 5: none of its records received. */
    final UUID none = UUID.randomUUID();

    /** 1 stored, its total not received. */
    final UUID noTotal = UUID.randomUUID();

    /** 2 of 2 stored, not completed. */
    final UUID allStored = UUID.randomUUID();

    /** 1 of 4 stored, 2 not received. */
    final UUID missing = UUID.randomUUID();

    /** 0 of 2, one claimed and live: being worked on. */
    final UUID working = UUID.randomUUID();

    /** Complete. */
    final UUID complete = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    StuckCommandTest()
    {
    }

    /**
     * Fills a registry with an item of each kind, claimed by two pods
     * whose owners are named as the stages name them.
     *
     * @throws Exception if the setup fails
     */
    @BeforeEach
    void registry() throws Exception
    {
        schema = StageTestSupport.freshSchema();
        StageRegistry a = StageTestSupport.registry(schema,
                "convert-7c9-abcde/0/" + UUID.randomUUID());
        StageRegistry b = StageTestSupport.registry(schema,
                "convert-7c9-fghij/1/" + UUID.randomUUID());

        a.expect(expired, 3);
        store(a, expired, "e1");
        a.claim("e2", expired, Duration.ofMillis(1));
        a.expect(none, 5);
        store(a, noTotal, "t1");
        a.expect(allStored, 2);
        store(b, allStored, "s1");
        store(b, allStored, "s2");
        a.expect(missing, 4);
        store(b, missing, "m1");
        b.claim("m2", missing, Duration.ofMillis(1));
        b.claim("m3", missing, Duration.ofMillis(1));
        b.expect(working, 2);
        b.claim("w1", working, Duration.ofMinutes(5));
        a.expect(complete, 1);
        store(a, complete, "c1");
        a.markCompleted(complete);
        Thread.sleep(50);
    }

    /**
     * Claims and stores one record.
     *
     * @param registry the registry, as a pod
     * @param itemId   its item
     * @param recordId the record
     */
    static void store(StageRegistry registry, UUID itemId, String recordId)
    {
        registry.claim(recordId, itemId, Duration.ofMinutes(5));
        registry.store(List.of(StageAdminTest.entry(recordId, itemId,
                false)));
    }

    /**
     * Runs {@code stuck} as operator {@code alice}.
     *
     * @param options its options
     * @return what it did
     */
    StageAdminTest.Result stuck(String... options)
    {
        List<String> all = new ArrayList<>(List.of("stuck"));
        all.addAll(List.of(options));
        all.add("--sdh.lease.jdbc.url="
                + StageTestSupport.POSTGRES.getJdbcUrl("postgres", "postgres"));
        all.add("--sdh.lease.jdbc.username=postgres");
        all.add("--sdh.lease.jdbc.password=postgres");
        all.add("--sdh.lease.jdbc.table=" + schema + "_lease");
        all.add("--sdh.registry-schema=" + schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = new AdminCli<>(new AdminCli.Stage<>("test-stage.jar",
                "test-stage", StageAdminTest.Configuration.class,
                StageAdminTest.TestSettings.class,
                StageAdminTest.TestSettings::telemetry,
                StageAdminTest.Context::new),
                List.of(new StuckCommand<StageAdminTest.Context>()))
                .run(all, new PrintStream(out, true, UTF_8),
                        new PrintStream(err, true, UTF_8),
                        Map.of(AdminCli.OPERATOR_ENV, "alice"));
        return new StageAdminTest.Result(status, out.toString(UTF_8),
                err.toString(UTF_8));
    }

    /**
     * With no idle time, every item not complete and not being worked on
     * is listed with why it waits and its reprocess line; the one with a
     * live claim and the complete one aren't. It is audited with the
     * count.
     */
    @Test
    void listsTheItemsNothingIsWorkingOnWithWhy()
    {
        StageAdminTest.Result result = stuck("--idle", "0");
        assertThat(result.status()).as(result.out() + result.err()).isZero();
        assertThat(result.out())
                .containsPattern("stuck items: +5 \\(none claimed, none"
                        + " stored for 0s\\)")
                .containsPattern(expired + " 1 of 3 stored, the last \\d+s"
                        + " ago; 1 left claimed past their claim: the sweep"
                        + " hands them in again")
                .contains(none + " 0 of 5 stored, none ever; 5 not received:"
                        + " the reconciler rebuilds them")
                .contains(noTotal + " 1 stored, of a total not received")
                .contains("its end from the stage before hasn't come")
                .contains(allStored + " 2 of 2 stored")
                .contains("all stored, not completed")
                .contains(missing + " 1 of 4 stored")
                .contains("2 left claimed past their claim")
                .doesNotContain(working + " 0 of 2")
                .doesNotContain(complete.toString());
        for (UUID item : List.of(expired, none, noTotal, allStored, missing))
        {
            assertThat(result.out())
                    .contains("sdh-admin reprocess --item-id " + item);
        }
        assertThat(audit.attributes("admin.stuck"))
                .containsEntry("sdh.operator", "alice")
                .containsEntry("sdh.stage", "convert")
                .containsEntry("sdh.count", 5L)
                .containsEntry("sdh.result", "ok");
    }

    /**
     * The claims are listed by item and pod, the longest expired first,
     * with their attempts; a live one says when it expires. No record is
     * named.
     */
    @Test
    void listsTheClaimsByItemAndPod()
    {
        String out = stuck("--idle", "0").out();
        assertThat(out)
                .containsPattern("claims: +3 items and pods, the longest"
                        + " expired first")
                .containsPattern(expired + " convert-7c9-abcde: 1 record,"
                        + " at most 1 attempt, expired \\d+s ago")
                .containsPattern(missing + " convert-7c9-fghij: 2 records,"
                        + " at most 1 attempt, expired \\d+s ago")
                .containsPattern(working + " convert-7c9-fghij: 1 record,"
                        + " at most 1 attempt, held, expires in \\d+[sm]")
                .doesNotContainPattern("\\b[emtswc]\\d\\b");
        assertThat(out.indexOf(working + " convert"))
                .as("the live claim comes last")
                .isGreaterThan(out.indexOf(missing + " convert"));
    }

    /**
     * By default an item counts as stuck only once nothing has been
     * stored for 10 minutes, so of these only the one with no records.
     * A bad {@code --idle} is a usage error.
     */
    @Test
    void waitsTheIdleTimeByDefault()
    {
        assertThat(stuck().out())
                .containsPattern("stuck items: +1 \\(none claimed, none"
                        + " stored for 10m\\)")
                .contains(none + " 0 of 5 stored, none ever")
                .doesNotContain(expired + " 1 of 3");
        StageAdminTest.Result bad = stuck("--idle", "soon");
        assertThat(bad.status()).isEqualTo(2);
        assertThat(bad.err())
                .contains("--idle must be a whole number of minutes");
    }
}
