/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link LocalDirectorySource}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LocalDirectorySourceTest
{

    /** The source directory, and a sibling to move items to. */
    @TempDir
    Path root;

    /**
     * Checked settings for a source under {@link #root}.
     *
     * @param access the access mode
     * @param after  the after-action
     * @param ready  the readiness mode
     * @return the settings
     */
    SourceSettings settings(String access, String after, String ready)
    {
        return SourceSettings.of(new LandingProperties.Source(
                root.resolve("in"), access, after, root.resolve("done"),
                ".landed", ready, Duration.ZERO, "*", true,
                Duration.ofSeconds(1)));
    }

    /**
     * Writes a file in the source.
     *
     * @param ref     its path within the source
     * @param content its content
     * @return the item as a listing would see it
     * @throws Exception if it can't be written
     */
    ListedItem write(String ref, String content) throws Exception
    {
        Path file = root.resolve("in").resolve(ref);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
        return new ListedItem(ref, Files.size(file),
                Files.getLastModifiedTime(file).toMillis());
    }

    /** Items are read from any offset, and a changed one is refused. */
    @Test
    void readsFromAnOffsetAndRefusesAChangedItem() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-only", null, "settled"));
        ListedItem item = write("sub/a.csv", "0123456789");
        try (InputStream in = source.open(item, 4))
        {
            assertThat(new String(in.readAllBytes(), UTF_8))
                    .isEqualTo("456789");
        }
        assertThatThrownBy(() -> source.open(new ListedItem("sub/a.csv", 9,
                item.version()), 0))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        assertThatThrownBy(() -> source.open(new ListedItem("gone.csv", 1,
                1), 0)).isInstanceOf(ItemLander.ItemChangedException.class);
        assertThatThrownBy(() -> source.open(new ListedItem("../x", 1, 1),
                0)).hasMessageContaining("outside the source");
    }

    /** A read-only source is never changed. */
    @Test
    void readOnlyLeavesItemsAlone() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-only", null, "settled"));
        ListedItem item = write("a.csv", "x");
        source.checkWritable();
        source.afterLanding(item);
        assertThat(root.resolve("in/a.csv")).exists();
    }

    /** Delete removes the item and its marker. */
    @Test
    void deleteRemovesTheItemAndItsMarker() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-write", "delete", "marker"));
        ListedItem item = write("a.csv", "x");
        write("a.csv.done", "");
        source.afterLanding(item);
        assertThat(root.resolve("in/a.csv")).doesNotExist();
        assertThat(root.resolve("in/a.csv.done")).doesNotExist();
    }

    /** Move keeps the relative path and never overwrites. */
    @Test
    void moveKeepsThePathAndNeverOverwrites() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-write", "move", "settled"));
        Files.createDirectories(root.resolve("in"));
        source.checkWritable();
        source.afterLanding(write("sub/a.csv", "first"));
        source.afterLanding(write("sub/a.csv", "second"));
        assertThat(root.resolve("done/sub/a.csv")).hasContent("first");
        assertThat(root.resolve("done/sub/a.csv.1")).hasContent("second");
        assertThat(root.resolve("in/sub/a.csv")).doesNotExist();
    }

    /** Rename adds the suffix beside the item. */
    @Test
    void renameAddsTheSuffix() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-write", "rename", "settled"));
        source.afterLanding(write("a.csv", "x"));
        assertThat(root.resolve("in/a.csv.landed")).hasContent("x");
    }

    /** An item that changed since it was listed is a new version: kept. */
    @Test
    void leavesAChangedItemForItsTurn() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-write", "delete", "settled"));
        ListedItem item = write("a.csv", "x");
        write("a.csv", "longer now");
        source.afterLanding(item);
        assertThat(root.resolve("in/a.csv")).hasContent("longer now");
    }

    /** The probe finds a source that can't be written, and its reach. */
    @Test
    void probesWritingAndReach() throws Exception
    {
        LocalDirectorySource source = new LocalDirectorySource(
                settings("read-write", "delete", "settled"));
        assertThat(source.reachable()).isFalse();
        Path in = Files.createDirectories(root.resolve("in"));
        assertThat(source.reachable()).isTrue();
        source.checkWritable();
        try (var listing = Files.list(in))
        {
            assertThat(listing).as("the probe cleans up").isEmpty();
        }
        Files.setPosixFilePermissions(in,
                PosixFilePermissions.fromString("r-xr-xr-x"));
        try
        {
            assertThatThrownBy(source::checkWritable)
                    .isInstanceOf(IntakeNotWritableException.class)
                    .hasMessageContaining("sdh.landing.source.path")
                    .hasMessageNotContaining(in.toString());
        }
        finally
        {
            Files.setPosixFilePermissions(in,
                    PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }
}
