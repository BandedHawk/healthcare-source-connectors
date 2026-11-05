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
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BucketSource}'s listing rules, reads and after-actions,
 * against a bucket held in memory.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class BucketSourceTest
{

    /**
     * Settings for a bucket source on {@code /in}.
     *
     * @param access    the access mode
     * @param after     the after-action, or null for the default
     * @param ready     {@code settled} or {@code marker}
     * @param recursive whether to look under deeper prefixes
     * @return the checked settings
     */
    static SourceSettings settings(String access, String after, String ready,
            boolean recursive)
    {
        return SourceSettings.of(new LandingProperties.Source(Path.of("/in"),
                access, after, Path.of("/in/done"), ".landed", ready,
                Duration.ZERO, "**/*", recursive, Duration.ofSeconds(1),
                "s3", Duration.ofMinutes(5), null, null,
                new LandingProperties.Bucket("b", null, "eu-west-1", false,
                        null, null, null, null, false, Duration.ofSeconds(1),
                        Duration.ofSeconds(1)), null));
    }

    /**
     * Lists flat or deep, leaving out hidden names, the move-to prefix,
     * keys outside the source and prefix placeholders, oldest first.
     *
     * @throws Exception if the test fails
     */
    @Test
    void appliesTheListingRules() throws Exception
    {
        Memory deep = new Memory(settings("read-write", "move", "settled",
                true));
        deep.put("in/b.csv", "b", 2);
        deep.put("in/a.csv", "a", 1);
        deep.put("in/sub/c.csv", "c", 3);
        deep.put("in/.hidden/d.csv", "d", 4);
        deep.put("in/.e.csv", "e", 5);
        deep.put("in/done/f.csv", "f", 6);
        deep.put("in/sub/", "", 7);
        deep.put("other/g.csv", "g", 8);
        deep.put("in2/h.csv", "h", 9);
        assertThat(deep.list()).extracting(ListedItem::ref)
                .containsExactly("a.csv", "b.csv", "sub/c.csv");
        assertThat(deep.lastFlat).isFalse();

        Memory flat = new Memory(settings("read-only", null, "settled",
                false));
        flat.objects.putAll(deep.objects);
        assertThat(flat.list()).extracting(ListedItem::ref)
                .containsExactly("a.csv", "b.csv");
        assertThat(flat.lastFlat).isTrue();
    }

    /**
     * An item's version holds its time and tag; it is read from an
     * offset, at its size reads nothing, and once it has changed is
     * refused.
     *
     * @throws Exception if the test fails
     */
    @Test
    void readsFromAnOffsetAndRefusesAChangedItem() throws Exception
    {
        Memory source = new Memory(settings("read-only", null, "settled",
                false));
        source.put("in/a.csv", "0123456789", 1000);
        ListedItem item = source.list().getFirst();
        assertThat(item.versionText()).isEqualTo("1000:t1");
        try (InputStream input = source.open(item, 4))
        {
            assertThat(new String(input.readAllBytes(), UTF_8))
                    .isEqualTo("456789");
        }
        try (InputStream input = source.open(item, 10))
        {
            assertThat(input.readAllBytes()).isEmpty();
        }
        source.put("in/a.csv", "0123456789", 1000);
        assertThatThrownBy(() -> source.open(item, 0))
                .isInstanceOf(ItemLander.ItemChangedException.class);
        assertThatThrownBy(() -> source.open(item, 10))
                .isInstanceOf(ItemLander.ItemChangedException.class);
    }

    /**
     * {@code move} copies to a free key under {@code move-to}, then
     * deletes the item and its marker, only if it is unchanged.
     *
     * @throws Exception if the test fails
     */
    @Test
    void movesLandedItemsAndTheirMarkers() throws Exception
    {
        Memory source = new Memory(settings("read-write", "move", "marker",
                true));
        source.put("in/sub/a.csv", "a", 1);
        source.put("in/sub/a.csv.done", "", 1);
        source.put("in/done/sub/a.csv", "old", 1);
        ListedItem item = source.list().getFirst();
        source.afterLanding(item);
        assertThat(source.objects.keySet()).containsExactly(
                "in/done/sub/a.csv", "in/done/sub/a.csv.1");
        assertThat(source.text("in/done/sub/a.csv.1")).isEqualTo("a");

        source.put("in/b.csv", "b", 2);
        source.put("in/b.csv.done", "", 2);
        ListedItem changed = source.list().getFirst();
        source.put("in/b.csv", "b2", 3);
        source.afterLanding(changed);
        assertThat(source.objects).containsKey("in/b.csv");
    }

    /**
     * {@code rename} adds the suffix, and {@code delete} deletes; renamed
     * items aren't listed again. A replaces marker is never an item, is
     * found beside its item, and is deleted with it.
     *
     * @throws Exception if the test fails
     */
    @Test
    void renamesOrDeletesLandedItems() throws Exception
    {
        Memory renaming = new Memory(settings("read-write", "rename",
                "settled", false));
        renaming.put("in/a.csv", "a", 1);
        renaming.afterLanding(renaming.list().getFirst());
        assertThat(renaming.objects.keySet())
                .containsExactly("in/a.csv.landed");
        assertThat(renaming.list()).isEmpty();

        Memory deleting = new Memory(settings("read-write", "delete",
                "settled", false));
        deleting.put("in/a.csv", "a", 1);
        deleting.put("in/a.csv.sdh-replaces", "an item ID", 2);
        ListedItem item = deleting.list().getFirst();
        assertThat(deleting.list()).extracting(ListedItem::ref)
                .containsExactly("a.csv");
        ListedItem marker = deleting.replacesMarker(item).orElseThrow();
        try (var input = deleting.open(marker, 0))
        {
            assertThat(new String(input.readAllBytes()))
                    .isEqualTo("an item ID");
        }
        assertThat(deleting.replacesMarker(new ListedItem("b.csv", 1,
                1))).isEmpty();
        deleting.afterLanding(item);
        assertThat(deleting.objects).isEmpty();
    }

    /**
     * The probe writes and deletes a hidden object under the source and
     * under {@code move-to}; a refusal is reported as not writable, and a
     * network failure as unavailable.
     *
     * @throws Exception if the test fails
     */
    @Test
    void probesTheSourceAndMoveTo() throws Exception
    {
        Memory source = new Memory(settings("read-write", "move", "settled",
                false));
        source.checkWritable();
        assertThat(source.probed).hasSize(2).allSatisfy(key -> assertThat(key)
                .matches("in/(done/)?\\.sdh-probe-.*"));
        assertThat(source.objects).isEmpty();

        source.failure = new Refused(IntakeProblem.PERMISSION);
        assertThatThrownBy(source::checkWritable)
                .isInstanceOf(IntakeNotWritableException.class);
        source.failure = new Refused(IntakeProblem.NETWORK);
        assertThatThrownBy(source::checkWritable)
                .isInstanceOf(IntakeUnavailableException.class);
    }

    /**
     * A read-write bucket is claimed by one node through the claim object
     * under its prefix, which is never listed; another node is refused,
     * and a store that can't be reached is unavailable. A read-only
     * bucket has no claim.
     *
     * @throws Exception if the test fails
     */
    @Test
    void claimsAReadWriteBucket() throws Exception
    {
        Memory source = new Memory(settings("read-write", "delete",
                "settled", false));
        InboundDirectoryClaim.Owner owner = new InboundDirectoryClaim.Owner(
                "landing-a", "lease-a", "cat", "ns");
        InboundDirectoryClaim claim = InboundDirectoryClaim.claim(
                source.claimFile().orElseThrow(), owner);
        assertThat(source.text("in/" + InboundDirectoryClaim.FILE))
                .contains("connector-id=landing-a");
        claim.verify();
        assertThat(source.list()).isEmpty();

        assertThatThrownBy(() -> InboundDirectoryClaim.claim(
                source.claimFile().orElseThrow(),
                new InboundDirectoryClaim.Owner("landing-b", "lease-b", "cat",
                        "ns")))
                .isInstanceOf(InboundDirectoryClaim.ClaimedException.class);

        source.objects.clear();
        source.bytes.clear();
        claim.verify();
        assertThat(source.text("in/" + InboundDirectoryClaim.FILE))
                .contains("connector-id=landing-a");

        source.failure = new Refused(IntakeProblem.NETWORK);
        assertThatThrownBy(claim::verify)
                .isInstanceOf(IntakeUnavailableException.class);
        assertThat(new Memory(settings("read-only", null, "settled", false))
                .claimFile()).isEmpty();
    }

    /**
     * A failed call says why and marks the store unreachable, and so does
     * a read that breaks part-way; the next listing that works marks it
     * reachable.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reportsFailuresAndBrokenReads() throws Exception
    {
        Memory source = new Memory(settings("read-only", null, "settled",
                false));
        source.put("in/a.csv", "0123456789", 1);
        ListedItem item = source.list().getFirst();
        source.failure = new Refused(IntakeProblem.AUTH);
        assertThatThrownBy(source::list)
                .isInstanceOfSatisfying(IntakeUnavailableException.class,
                        failure -> assertThat(failure.problem())
                                .isEqualTo(IntakeProblem.AUTH));
        assertThat(source.reachable()).isFalse();
        source.failure = null;
        assertThat(source.list()).containsExactly(item);
        assertThat(source.reachable()).isTrue();

        source.breakReads = true;
        try (InputStream input = source.open(item, 0))
        {
            assertThatThrownBy(input::readAllBytes)
                    .isInstanceOfSatisfying(IntakeUnavailableException.class,
                            failure -> assertThat(failure.problem())
                                    .isEqualTo(IntakeProblem.NETWORK))
                    .hasMessageNotContaining("a.csv");
        }
        assertThat(source.reachable()).isFalse();
    }

    /** A refusal from the store, which says why. */
    static final class Refused extends RuntimeException
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;

        /** Why it was refused. */
        final transient IntakeProblem problem;

        /**
         * Creates a refusal.
         *
         * @param problem why
         */
        Refused(IntakeProblem problem)
        {
            super("in/a.csv refused");
            this.problem = problem;
        }
    }

    /** A bucket held in memory. */
    static final class Memory extends BucketSource
    {

        /** The objects, by key. */
        final Map<String, Found> objects = new TreeMap<>();

        /** Their bytes, by key. */
        final Map<String, byte[]> bytes = new TreeMap<>();

        /** The probe keys written. */
        final List<String> probed = new ArrayList<>();

        /** What every call throws; null for none. */
        RuntimeException failure;

        /** Whether reads break after their first byte. */
        boolean breakReads;

        /** Whether the last listing was flat. */
        boolean lastFlat;

        /** Gives each write a new tag. */
        int writes;

        /**
         * Creates the bucket.
         *
         * @param settings the source's settings
         */
        Memory(SourceSettings settings)
        {
            super(settings);
        }

        /**
         * Writes an object.
         *
         * @param key      its key
         * @param content  its content
         * @param modified its time
         */
        void put(String key, String content, long modified)
        {
            byte[] data = content.getBytes(UTF_8);
            objects.put(key, new Found(key, data.length, modified,
                    "t" + ++writes));
            bytes.put(key, data);
        }

        /**
         * An object's content.
         *
         * @param key its key
         * @return its content
         */
        String text(String key)
        {
            return new String(bytes.get(key), UTF_8);
        }

        /** Throws the failure, if there is one. */
        private void check()
        {
            if (failure != null)
            {
                throw failure;
            }
        }

        @Override
        protected List<Found> objects(String keyPrefix, boolean flat)
        {
            check();
            lastFlat = flat;
            List<Found> found = new ArrayList<>();
            for (Found object : objects.values())
            {
                String rest = object.key().startsWith(keyPrefix)
                        ? object.key().substring(keyPrefix.length()) : null;
                if (rest != null && !(flat && rest.contains("/")))
                {
                    found.add(object);
                }
            }
            return found;
        }

        @Override
        protected Found stat(String key)
        {
            check();
            return objects.get(key);
        }

        @Override
        protected InputStream read(String key, ListedItem item, long offset)
                throws IOException
        {
            check();
            Found found = objects.get(key);
            if (found == null || !found.tag().equals(item.tag()))
            {
                throw new ItemLander.ItemChangedException();
            }
            byte[] data = bytes.get(key);
            if (breakReads)
            {
                return new InputStream()
                {
                    @Override
                    public int read() throws IOException
                    {
                        throw new IOException("in/a.csv broke");
                    }
                };
            }
            return new ByteArrayInputStream(Arrays.copyOfRange(data,
                    (int) offset, data.length));
        }

        @Override
        protected boolean copy(String from, String tag, long size,
                String target)
        {
            check();
            Found found = objects.get(from);
            if (found == null || !found.tag().equals(tag))
            {
                return false;
            }
            objects.put(target, new Found(target, found.size(),
                    found.modified(), "t" + ++writes));
            bytes.put(target, bytes.get(from));
            return true;
        }

        @Override
        protected void delete(String key, String tag)
        {
            check();
            Found found = objects.get(key);
            if (found != null && (tag == null || found.tag().equals(tag)))
            {
                objects.remove(key);
                bytes.remove(key);
            }
        }

        @Override
        protected void putEmpty(String key)
        {
            check();
            probed.add(key);
            put(key, "", 0);
        }

        @Override
        protected byte[] readSmall(String key)
        {
            check();
            return bytes.get(key);
        }

        @Override
        protected boolean createNew(String key, byte[] content)
        {
            check();
            if (objects.containsKey(key))
            {
                return false;
            }
            put(key, new String(content, UTF_8), 0);
            return true;
        }

        @Override
        protected IntakeProblem classify(Exception failure)
        {
            return failure instanceof Refused refused ? refused.problem
                    : IntakeProblem.NETWORK;
        }

        @Override
        protected void closeClient()
        {
        }
    }
}
