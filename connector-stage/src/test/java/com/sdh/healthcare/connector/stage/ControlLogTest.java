/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.maintenance.ListingFileIO;
import com.sdh.healthcare.connector.core.maintenance.VersionedFileIO;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.junit.jupiter.api.Test;

/**
 * A scope's control changes kept in the store (DESIGN.md §18.6): one
 * object each, written once, read back oldest first, beside the tables and
 * apart from another scope's.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlLogTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlLogTest()
    {
    }

    /**
     * Each change is kept once, by its kind and operation; they are read
     * back oldest first, with their fields; another scope's are apart.
     * An operation's ID that isn't plain is named by a UUID made from it.
     */
    @Test
    void keepsEachChangeOnceAndReadsThemBack()
    {
        String namespace = "memory://wh-" + UUID.randomUUID() + "/ns";
        ControlLog log = new ControlLog(new ListingFileIO(), namespace,
                "convert/test");
        log.keep(ControlLog.Kind.PURGE, "op-1", "alice",
                Map.of("item", "i-1", "ticket", "CHG-1"));
        log.keep(ControlLog.Kind.PURGE, "op-1", "bob",
                Map.of("item", "i-2", "ticket", "CHG-2"));
        log.keep(ControlLog.Kind.ROTATE, "op 2/odd", "alice",
                Map.of("to", "kek/2"));
        new ControlLog(new ListingFileIO(), namespace, "convert/other")
                .keep(ControlLog.Kind.ERASE, "op-3", "carol", Map.of());

        // Two kept in the same microsecond are ordered by ID instead.
        assertThat(log.entries()).hasSize(2).isSortedAccordingTo(
                Comparator.comparing(ControlLog.Entry::at)
                        .thenComparing(ControlLog.Entry::id));
        assertThat(log.entries()).filteredOn(entry -> entry.kind()
                == ControlLog.Kind.PURGE).singleElement().satisfies(entry ->
        {
            assertThat(entry.kind()).isEqualTo(ControlLog.Kind.PURGE);
            assertThat(entry.by()).isEqualTo("alice");
            assertThat(entry.scope()).isEqualTo("convert/test");
            assertThat(entry.fields()).containsEntry("item", "i-1")
                    .containsEntry("ticket", "CHG-1");
        });
        assertThat(log.entries()).anySatisfy(entry ->
                assertThat(entry.id()).isEqualTo("op 2/odd"));
        assertThat(VersionedFileIO.under(namespace + "/"
                + ControlLog.DIRECTORY + "/")).hasSize(3);
    }

    /**
     * A field's name is letters; an entry that can't be read, or a store
     * that can't list, is said to be unreadable.
     *
     * @throws IOException if the test's object can't be written
     */
    @Test
    void refusesWhatItCantKeepOrRead() throws IOException
    {
        String namespace = "memory://wh-" + UUID.randomUUID() + "/ns";
        ControlLog log = new ControlLog(new ListingFileIO(), namespace, "s");
        assertThatThrownBy(() -> log.keep(ControlLog.Kind.PURGE, "op", "a",
                Map.of("not a name", "x")))
                .isInstanceOf(IllegalArgumentException.class);

        String bad = namespace + "/" + ControlLog.DIRECTORY + "/cw/x.json";
        try (OutputStream out = new ListingFileIO().newOutputFile(bad)
                .create())
        {
            out.write("not json".getBytes(StandardCharsets.UTF_8));
        }
        assertThatThrownBy(log::entries)
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> new ControlLog(new InMemoryFileIO(),
                namespace, "s").entries())
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * The directory a table's location is in is its namespace's.
     */
    @Test
    void findsATablesNamespace()
    {
        assertThat(ControlLog.namespaceOf("s3://bucket/wh/ns/records/"))
                .isEqualTo("s3://bucket/wh/ns");
        assertThat(ControlLog.namespaceOf("memory://wh/ns/records"))
                .isEqualTo("memory://wh/ns");
        assertThatThrownBy(() -> ControlLog.namespaceOf("s3://bucket"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
