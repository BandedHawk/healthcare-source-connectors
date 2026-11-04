/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandingCheckpoint;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link IntakeService} on a real lander, ledger and directory,
 * an in-memory catalog, and a publisher that keeps what it is sent.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IntakeServiceTest
{

    /** The intake under test. */
    static final String INTAKE = "claims";

    /** The KEK that wraps the data keys. */
    static final String KEK = "landed-items-kek/1";

    /** A quick retry policy with one attempt. */
    static final RetryPolicy RETRY =
            new RetryPolicy(1, Duration.ofMillis(1), Duration.ofMillis(1));

    /** Thrown from the commit hook to stand in for a crash. */
    static final class Crash extends Error
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;
    }

    /** The source directory's parent. */
    @TempDir
    Path root;

    /** The landed items table. */
    Table items;

    /** The ledger table. */
    Table ledgerTable;

    /** Wraps the data keys. */
    AesKeyWrapProvider keys;

    /** Makes the hashes and MACs. */
    final SectionIds ids = new SectionIds(new byte[32]);

    /** The last lease epoch handed out. */
    long epoch;

    /** The signing key pair. */
    KeyPair signing;

    /** The recipient's key pair. */
    KeyPair recipient;

    /** What the publisher was sent. */
    final List<OutboundMessage> sent = new CopyOnWriteArrayList<>();

    /** How many sends should fail before they start working. */
    final AtomicInteger failSends = new AtomicInteger();

    /** The reasons given to the exit hook. */
    final List<String> fatal = new CopyOnWriteArrayList<>();

    /** The fake clock for readiness, in nanoseconds. */
    final AtomicLong now = new AtomicLong();

    /**
     * Creates the catalog, tables and keys.
     *
     * @throws Exception if a key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        items = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        ledgerTable = LandingTables.ledger(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        keys = new AesKeyWrapProvider(Map.of(KEK, generator.generateKey()));
        KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp256r1"));
        signing = ec.generateKeyPair();
        recipient = ec.generateKeyPair();
        Files.createDirectories(root.resolve("in"));
    }

    /**
     * Checked settings for a source under {@link #root}.
     *
     * @param access the access mode
     * @return the settings
     */
    SourceSettings settings(String access)
    {
        return SourceSettings.of(new LandingProperties.Source(
                root.resolve("in"), access, null, null, ".landed", "settled",
                Duration.ZERO, "*", false, Duration.ofSeconds(1), "directory",
                Duration.ofMinutes(5), null, null, null, null));
    }

    /**
     * A committer under a new lease epoch, as after a restart.
     *
     * @return the committer
     */
    FencedCommitter committer()
    {
        long ours = ++epoch;
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return ours;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(items);
        committer.establish(ledgerTable);
        return committer;
    }

    /**
     * A lander with small chunks, so a few bytes take several commits.
     *
     * @param committer makes the commits
     * @return the lander
     */
    ItemLander lander(FencedCommitter committer)
    {
        return new ItemLander(items, committer, keys, ids,
                ItemLander.Settings.defaults(INTAKE, KEK).withRetry(RETRY)
                        .withSizes(4, 8, 8), Clock.systemUTC());
    }

    /**
     * A service as a fresh start would make it.
     *
     * @param source    where items come from
     * @param lander    the lander, under its committer
     * @param committer the lander's committer
     * @param settle    how long items must settle
     * @return the service
     * @throws Exception if the ledger can't be loaded
     */
    IntakeService service(IntakeSource source, ItemLander lander,
            FencedCommitter committer, Duration settle) throws Exception
    {
        LandingLedger ledger = new LandingLedger(ledgerTable, committer,
                RETRY);
        Announcer announcer = new Announcer(new MessageSealer(
                new ECDSASigner((ECPrivateKey) signing.getPrivate()),
                "connector", recipient.getPublic(), "downstream"),
                message ->
                {
                    if (failSends.getAndDecrement() > 0)
                    {
                        return CompletableFuture.failedFuture(
                                new IOException("broker down"));
                    }
                    sent.add(message);
                    return CompletableFuture.completedFuture(null);
                }, "sdh", LandingTables.DEFAULT_NAMESPACE, RETRY,
                Duration.ofSeconds(5));
        return new IntakeService(INTAKE, source,
                new Readiness(settle, now::get),
                new IntakeService.Storage(lander, ledger,
                        ledger.load(INTAKE), ids),
                announcer, null, fatal::add, Clock.systemUTC(),
                Duration.ofMillis(1));
    }

    /**
     * A service on a fresh committer and lander.
     *
     * @param source where items come from
     * @param settle how long items must settle
     * @return the service
     * @throws Exception if the ledger can't be loaded
     */
    IntakeService service(IntakeSource source, Duration settle)
            throws Exception
    {
        FencedCommitter committer = committer();
        return service(source, lander(committer), committer, settle);
    }

    /**
     * Writes a file in the source.
     *
     * @param name    its name
     * @param content its content
     * @return the file as a listing sees it
     * @throws Exception if it can't be written
     */
    ListedItem write(String name, String content) throws Exception
    {
        Path file = root.resolve("in").resolve(name);
        Files.writeString(file, content);
        return listed(name);
    }

    /**
     * A file as a listing sees it now.
     *
     * @param name its name
     * @return the listed item
     * @throws Exception if its attributes can't be read
     */
    ListedItem listed(String name) throws Exception
    {
        Path file = root.resolve("in").resolve(name);
        return new ListedItem(name, Files.size(file),
                Files.getLastModifiedTime(file).toMillis());
    }

    /**
     * Opens what was sent.
     *
     * @return the item.landed messages, in order
     * @throws Exception if one can't be opened
     */
    List<ConnectorMessage.ItemLanded> landed() throws Exception
    {
        MessageOpener opener = new MessageOpener(
                Map.of("downstream", recipient.getPrivate()),
                Map.of("connector", (ECPublicKey) signing.getPublic()));
        List<ConnectorMessage.ItemLanded> messages = new ArrayList<>();
        for (OutboundMessage message : sent)
        {
            assertThat(message.type()).isEqualTo(MessageType.ITEM_LANDED);
            messages.add((ConnectorMessage.ItemLanded)
                    opener.open(message.body()));
        }
        return messages;
    }

    /**
     * A replaces marker beside an item links it to an earlier item of this
     * intake, and goes with it from a read-write source; one naming an
     * unknown item, or holding something else, is left out, and the item
     * is landed all the same. A marker is never listed as an item.
     *
     * @throws Exception if the test fails
     */
    @Test
    void linksAnItemToTheOneItsMarkerNames() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-write")),
                Duration.ZERO);
        service.recoverAtStartup();
        service.onListed(write("a.csv", "1,first\n"));
        UUID earlier = landed().getFirst().fileId();

        Files.writeString(root.resolve("in/b.csv.sdh-replaces"),
                " " + earlier + "\n");
        service.onListed(write("b.csv", "1,second\n"));
        Files.writeString(root.resolve("in/c.csv.sdh-replaces"),
                UUID.randomUUID().toString());
        service.onListed(write("c.csv", "1,third\n"));
        Files.writeString(root.resolve("in/d.csv.sdh-replaces"), "a.csv");
        service.onListed(write("d.csv", "1,fourth\n"));

        List<ConnectorMessage.ItemLanded> messages = landed();
        assertThat(messages).hasSize(4);
        assertThat(messages.get(1).replaces()).isEqualTo(earlier);
        assertThat(messages.get(2).replaces()).isNull();
        assertThat(messages.get(3).replaces()).isNull();
        assertThat(root.resolve("in")).isEmptyDirectory();
        assertThat(new com.sdh.healthcare.connector.landing.route.IntakeRoute(
                settings("read-only"), service).uri())
                .contains("antExclude=**/*.sdh-replaces");
        assertThat(fatal).isEmpty();
    }

    /**
     * A settled file is landed, announced and deleted from a read-write
     * source.
     */
    @Test
    void landsAnnouncesAndDeletesASettledFile() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-write")),
                Duration.ofSeconds(10));
        service.recoverAtStartup();
        ListedItem item = write("a.csv", "1,first\n2,second\n");
        service.onListed(item);
        assertThat(sent).as("not settled yet").isEmpty();

        now.addAndGet(Duration.ofSeconds(10).toNanos());
        service.onListed(item);
        List<ConnectorMessage.ItemLanded> messages = landed();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).intake()).isEqualTo(INTAKE);
        assertThat(messages.get(0).size()).isEqualTo(item.size());
        assertThat(messages.get(0).kind()).isEqualTo("file");
        assertThat(sent.get(0).messageId())
                .isEqualTo("il-" + messages.get(0).fileId());
        assertThat(root.resolve("in/a.csv")).doesNotExist();
        assertThat(new LandingLedger(ledgerTable, committer(), RETRY)
                .contains(messages.get(0).fileId())).isTrue();
        assertThat(fatal).isEmpty();
    }

    /**
     * A read-only source lands each version of an item once, and the same
     * content under another name again.
     */
    @Test
    void readOnlyLandsEachVersionOnce() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-only")),
                Duration.ZERO);
        ListedItem item = write("a.csv", "same");
        service.onListed(item);
        service.onListed(item);
        service.onListingDone();
        service.onListed(item);
        assertThat(sent).hasSize(1);

        service.onListed(write("copy.csv", "same"));
        assertThat(sent).as("same content, another name").hasSize(2);

        Path file = root.resolve("in/a.csv");
        Files.setLastModifiedTime(file,
                FileTime.from(Instant.now().plusSeconds(60)));
        service.onListed(listed("a.csv"));
        assertThat(sent).as("a new version").hasSize(3);
        assertThat(file).exists();
        List<ConnectorMessage.ItemLanded> messages = landed();
        assertThat(messages.get(0).itemDigest())
                .isEqualTo(messages.get(1).itemDigest())
                .isEqualTo(messages.get(2).itemDigest());

        IntakeService restarted = service(
                new LocalDirectorySource(settings("read-only")),
                Duration.ZERO);
        restarted.recoverAtStartup();
        int resent = sent.size();
        restarted.onListed(listed("a.csv"));
        restarted.onListed(listed("copy.csv"));
        assertThat(sent).as("the ledger survives a restart")
                .hasSize(resent);
    }

    /**
     * A message the broker didn't take is sent again before anything
     * else, and the item stays in the source until it is.
     */
    @Test
    void retriesTheMessageBeforeTheNextItem() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-write")),
                Duration.ZERO);
        ListedItem item = write("a.csv", "x");
        failSends.set(1);
        service.onListed(item);
        assertThat(sent).isEmpty();
        assertThat(root.resolve("in/a.csv")).exists();

        service.onListed(item);
        assertThat(sent).hasSize(1);
        assertThat(root.resolve("in/a.csv")).doesNotExist();
        assertThat(fatal).isEmpty();
    }

    /**
     * An item landed before a crash, but not removed from the source, is
     * removed later without being landed again.
     */
    @Test
    void finishesTheSourceSideOfALandedItem() throws Exception
    {
        LocalDirectorySource directory =
                new LocalDirectorySource(settings("read-write"));
        AtomicInteger failRemovals = new AtomicInteger(1);
        IntakeSource flaky = new IntakeSource()
        {
            @Override
            public InputStream open(ListedItem item, long offset)
                    throws IOException
            {
                return directory.open(item, offset);
            }

            @Override
            public void afterLanding(ListedItem item) throws IOException
            {
                if (failRemovals.getAndDecrement() > 0)
                {
                    throw new IOException("share went away");
                }
                directory.afterLanding(item);
            }

            @Override
            public boolean reachable()
            {
                return true;
            }

            @Override
            public void checkWritable()
            {
            }
        };
        IntakeService service = service(flaky, Duration.ZERO);
        ListedItem item = write("a.csv", "x");
        service.onListed(item);
        assertThat(root.resolve("in/a.csv")).exists();
        service.onListed(item);
        assertThat(root.resolve("in/a.csv")).doesNotExist();
        assertThat(sent).hasSize(1);
    }

    /**
     * After a crash part-way through an item, a restart resumes it under
     * the same ID.
     */
    @Test
    void resumesAnUnfinishedItemAfterACrash() throws Exception
    {
        FencedCommitter first = committer();
        ItemLander crashing = lander(first);
        crashing.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        IntakeService service = service(
                new LocalDirectorySource(settings("read-write")), crashing,
                first, Duration.ZERO);
        ListedItem item = write("big.csv", "0123456789abcdefghijklmnop");
        service.onListed(item);
        assertThat(fatal).containsExactly("Crash");
        LandingCheckpoint unfinished = crashing.checkpoint().orElseThrow();
        assertThat(unfinished.done()).isFalse();

        IntakeService restarted = service(
                new LocalDirectorySource(settings("read-write")),
                Duration.ofHours(1));
        restarted.recoverAtStartup();
        restarted.onListed(write("other.csv", "later"));
        assertThat(sent).as("nothing else lands first").isEmpty();
        restarted.onListed(item);
        List<ConnectorMessage.ItemLanded> messages = landed();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).fileId()).isEqualTo(unfinished.itemId());
        assertThat(messages.get(0).size()).isEqualTo(item.size());
        assertThat(root.resolve("in/big.csv")).doesNotExist();
    }

    /**
     * A read that breaks part-way, as a network source's can, leaves the
     * item to resume at the next listing under the same ID, even with the
     * listing's end in between; nothing stops the process.
     *
     * @throws Exception if the test fails
     */
    @Test
    void resumesAnItemWhoseReadBroke() throws Exception
    {
        LocalDirectorySource directory =
                new LocalDirectorySource(settings("read-write"));
        AtomicInteger breaks = new AtomicInteger(1);
        IntakeSource flaky = new IntakeSource()
        {
            @Override
            public InputStream open(ListedItem item, long offset)
                    throws IOException
            {
                InputStream input = directory.open(item, offset);
                if (breaks.getAndDecrement() <= 0)
                {
                    return input;
                }
                return new FilterInputStream(input)
                {
                    private int left = 12;

                    @Override
                    public int read(byte[] buffer, int from, int length)
                            throws IOException
                    {
                        if (left <= 0 && length > 0)
                        {
                            throw new IOException("connection reset");
                        }
                        int read = super.read(buffer, from,
                                Math.min(length, left));
                        left -= Math.max(read, 0);
                        return read;
                    }
                };
            }

            @Override
            public void afterLanding(ListedItem item) throws IOException
            {
                directory.afterLanding(item);
            }

            @Override
            public boolean reachable()
            {
                return true;
            }

            @Override
            public void checkWritable()
            {
            }
        };
        FencedCommitter committer = committer();
        ItemLander lander = lander(committer);
        IntakeService service = service(flaky, lander, committer,
                Duration.ZERO);
        ListedItem item = write("big.csv", "0123456789abcdefghijklmnop");
        service.onListed(item);
        LandingCheckpoint broken = lander.checkpoint().orElseThrow();
        assertThat(broken.done()).isFalse();
        service.onListingDone();
        assertThat(lander.checkpoint()).as("kept to resume").isPresent();

        service.onListed(write("other.csv", "later"));
        assertThat(sent).as("nothing else lands first").isEmpty();
        service.onListed(item);
        assertThat(fatal).isEmpty();
        assertThat(landed()).singleElement().satisfies(message ->
        {
            assertThat(message.fileId()).isEqualTo(broken.itemId());
            assertThat(message.size()).isEqualTo(item.size());
        });
        service.onListed(listed("other.csv"));
        assertThat(sent).hasSize(2);
    }

    /**
     * A remote source that can't be listed is reported with its reason,
     * once, and the report is cleared when a listing works again.
     *
     * @throws Exception if the test fails
     */
    @Test
    void reportsAFailedListingWithItsReason() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-only")),
                Duration.ZERO);
        service.onListingFailed(IntakeProblem.AUTH);
        service.onListingFailed(IntakeProblem.AUTH);
        assertThat(service.reachable()).isFalse();
        service.onListingDone();
        assertThat(service.reachable()).isTrue();
    }

    /**
     * An unfinished item missing from the first full listing is given up,
     * and the intake moves on.
     */
    @Test
    void abandonsAnUnfinishedItemThatHasGone() throws Exception
    {
        FencedCommitter first = committer();
        ItemLander crashing = lander(first);
        crashing.afterCommit(nextBlock ->
        {
            throw new Crash();
        });
        service(new LocalDirectorySource(settings("read-write")), crashing,
                first, Duration.ZERO)
                .onListed(write("big.csv", "0123456789abcdefghijklmnop"));
        Files.delete(root.resolve("in/big.csv"));

        FencedCommitter second = committer();
        ItemLander lander = lander(second);
        IntakeService restarted = service(
                new LocalDirectorySource(settings("read-write")), lander,
                second, Duration.ZERO);
        restarted.recoverAtStartup();
        restarted.onListingDone();
        assertThat(lander.checkpoint()).isEmpty();
        restarted.onListed(write("next.csv", "next"));
        assertThat(sent).hasSize(1);
    }

    /** An item that changed after it was listed is landed afresh later. */
    @Test
    void landsAChangedItemAfresh() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-only")),
                Duration.ZERO);
        ListedItem stale = write("a.csv", "short");
        write("a.csv", "longer content");
        service.onListed(stale);
        assertThat(sent).isEmpty();
        assertThat(fatal).isEmpty();
        service.onListed(listed("a.csv"));
        assertThat(landed()).singleElement().extracting(
                ConnectorMessage.ItemLanded::size).isEqualTo(14L);
    }

    /** An unreachable source is reported, and so is its return. */
    @Test
    void reportsAnUnreachableSource() throws Exception
    {
        IntakeService service = service(
                new LocalDirectorySource(settings("read-only")),
                Duration.ZERO);
        Files.delete(root.resolve("in"));
        service.onListingDone();
        assertThat(service.reachable()).isFalse();
        Files.createDirectories(root.resolve("in"));
        service.onListingDone();
        assertThat(service.reachable()).isTrue();
    }
}
