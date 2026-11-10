/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.SplitItemReport;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.landing.LandedItemSource;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.landing.LandedRefs;
import com.sdh.healthcare.connector.core.pipeline.SplitMessages;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.PartWay;
import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import com.sdh.healthcare.connector.stage.StageReleases;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.DrainAdmin;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * What the split stage's admin commands read from, each part opened only
 * when a command needs it. Strictly read-only: tables are loaded, never
 * created; ID keys are never created; leases are inspected, never taken;
 * the database connection is read-only (design §11.4). Only the commands
 * that change the stage's tables ({@code decline}, {@code reopen},
 * {@code correct}, {@code retry}, {@code release}, {@code reprocess}) write,
 * over a connection of their own.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class SplitAdminContext implements DrainAdmin, AutoCloseable
{

    /** The stage's settings. */
    private final ConnectorProperties properties;

    /** The lease database. */
    private final AdminDatabase database;

    /** The lease database, writing: for the fail store's changes. */
    private final AdminDatabase writing;

    /** The keys, once loaded. */
    private KeyMaterial keys;

    /** The stage's catalog, once built. */
    private Catalog catalog;

    /** Landing's catalog, once built; the stage's if it is the same. */
    private Catalog landingCatalog;

    /** The ID keys of {@code split_records}, once loaded. */
    private RecordIds splitIds;

    /** Landing's ID keys, once loaded. */
    private RecordIds landingIds;

    /**
     * Creates the context; nothing is opened yet.
     *
     * @param properties the stage's settings
     */
    SplitAdminContext(ConnectorProperties properties)
    {
        this.properties = properties;
        this.database = new AdminDatabase(properties.lease());
        this.writing = new AdminDatabase(properties.lease(), true);
    }

    /**
     * The stage's settings.
     *
     * @return the settings
     */
    ConnectorProperties properties()
    {
        return properties;
    }

    /**
     * The keys, loaded on first use.
     *
     * @return the keys
     * @throws Exception if they can't be loaded
     */
    KeyMaterial keys() throws Exception
    {
        if (keys == null)
        {
            keys = KeyMaterial.load(properties.crypto());
        }
        return keys;
    }

    /**
     * The stage's catalog, built on first use.
     *
     * @return the catalog
     * @throws IllegalArgumentException if no catalog is configured
     */
    Catalog catalog()
    {
        if (catalog == null)
        {
            Map<String, String> settings = properties.storage().catalog();
            if (settings == null || settings.isEmpty())
            {
                throw new IllegalArgumentException(
                        "sdh.storage.catalog is required");
            }
            catalog = CatalogUtil.buildIcebergCatalog(
                    properties.storage().catalogName(), settings, null);
        }
        return catalog;
    }

    /**
     * Landing's catalog: its own if configured, else the stage's.
     *
     * @return the catalog
     */
    Catalog landingCatalog()
    {
        if (landingCatalog == null)
        {
            Map<String, String> settings =
                    properties.landing().storage().catalog();
            landingCatalog = settings == null || settings.isEmpty()
                    ? catalog()
                    : CatalogUtil.buildIcebergCatalog(
                            properties.landing().storage().catalogName(),
                            settings, null);
        }
        return landingCatalog;
    }

    /**
     * Loads a table, which must exist.
     *
     * @param catalog   the catalog
     * @param namespace the namespace
     * @param name      the table
     * @return the table
     * @throws IllegalStateException if it doesn't exist
     */
    private static Table load(Catalog catalog, String namespace, String name)
    {
        try
        {
            return catalog.loadTable(TableIdentifier.of(namespace, name));
        }
        catch (NoSuchTableException failure)
        {
            throw new IllegalStateException("Table " + name
                    + " does not exist: the stage has never run here");
        }
    }

    /**
     * The stage's {@code split_records}.
     *
     * @return the table
     */
    Table splitRecords()
    {
        return load(catalog(), properties.storage().namespace(),
                IngestTables.SPLIT_RECORDS);
    }

    /**
     * Landing's {@code landed_items}.
     *
     * @return the table
     */
    Table landedItems()
    {
        return load(landingCatalog(), properties.landing().storage()
                .namespace(), LandingTables.LANDED_ITEMS);
    }

    /**
     * Landing's ID keys, which name a landed item's chunks.
     *
     * @return the keys
     * @throws Exception if they can't be loaded
     */
    RecordIds landingIds() throws Exception
    {
        if (landingIds == null)
        {
            landingIds = new RecordIds(
                    IdKeys.load(landedItems(), keys().wrap()));
        }
        return landingIds;
    }

    /**
     * The checkpoints of the stage's items, to load: they are MAC'd with
     * the ID key of {@code split_records}.
     *
     * @return the checkpoints
     * @throws Exception if the ID key can't be loaded
     */
    ItemCheckpoints checkpoints() throws Exception
    {
        if (splitIds == null)
        {
            splitIds = new RecordIds(
                    IdKeys.load(splitRecords(), keys().wrap()));
        }
        return new ItemCheckpoints(database.dataSource(),
                properties.stage().registrySchema(),
                new CheckpointCodec(splitIds), Duration.ofSeconds(10));
    }

    /**
     * The checkpoints of the stage's items, to number an item's end before
     * it is sent (STAGES.md §15.6e): over the connection that may write.
     *
     * @return the checkpoints
     * @throws Exception if the ID key can't be loaded
     */
    ItemCheckpoints checkpointsToChange() throws Exception
    {
        checkpoints();
        return new ItemCheckpoints(writing.dataSource(),
                properties.stage().registrySchema(),
                new CheckpointCodec(splitIds), Duration.ofSeconds(10));
    }

    @Override
    public String stage()
    {
        return ItemProcessor.STAGE;
    }

    /**
     * The split stage's fail store (STAGES.md §15.3), to read.
     *
     * @return the store
     */
    @Override
    public FailureStore failures()
    {
        return new FailureStore(database.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public FailureStore failuresToChange()
    {
        return new FailureStore(writing.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public KeyProvider reasonKeys() throws Exception
    {
        return keys().wrap();
    }

    @Override
    public String reasonKek()
    {
        return properties.crypto().keys().recordsKek();
    }

    @Override
    public boolean corrects()
    {
        return true;
    }

    /**
     * The largest item the stage splits: a correction is a piece of an
     * item, or the whole of one.
     */
    @Override
    public long maxCorrectionBytes()
    {
        String format = properties.split().format();
        return ConnectorRuntime.splitConfig(properties.split(), format)
                .limits().maxItemBytes();
    }

    @Override
    public CorrectionStore correctionsToChange()
    {
        return new CorrectionStore(writing.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public CorrectionStore corrections()
    {
        return new CorrectionStore(database.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    /**
     * What failed, decrypted: a piece's row of {@code split_records} as
     * cut from the item, or, for the item as a whole, the item as landed.
     */
    @Override
    public byte[] failedContent(UUID itemId, long seq) throws Exception
    {
        int max = (int) Math.min(Integer.MAX_VALUE - 8,
                maxCorrectionBytes());
        if (seq == FailureStore.WHOLE_ITEM)
        {
            try (LandedItemSource source = LandedItemSource.open(
                    landedItems(), itemId, keys().wrap()))
            {
                if (source.size() > max)
                {
                    throw new IllegalStateException("The item is larger"
                            + " than the stage splits");
                }
                return source.readFully(0, (int) source.size());
            }
        }
        Table table = splitRecords();
        table.refresh();
        // The newest generation's, if the piece failed in more than one.
        CommittedRecords.Row row = CommittedRecords.all(table, itemId)
                .stream()
                .filter(each -> each.seq() == seq && each.errorCode() != null)
                .reduce((older, newer) -> newer)
                .orElseThrow(() -> new IllegalStateException("The failed"
                        + " piece isn't in " + IngestTables.SPLIT_RECORDS));
        long snapshot = table.currentSnapshot().snapshotId();
        StorageRef ref = new StorageRef(properties.storage().catalogName(),
                properties.storage().namespace(), IngestTables.SPLIT_RECORDS,
                row.ingestedDate(), snapshot, row.kekRef());
        try (QueueReader reader = QueueReader.builder().catalog(catalog())
                .keyProvider(keys().wrap()).build())
        {
            return reader.readRecord(SplitMessages.recordSplit(itemId, row,
                    ref, LandedRefs.NONE), max);
        }
    }

    /**
     * Leaves the item to the stage's pods, which send its end again once
     * one of its failures changed since it was sent (STAGES.md §15.8.10
     * step 5): the admin sends nothing.
     */
    @Override
    public void changed(UUID itemId, AuditRecord audit, PrintStream out)
    {
        out.println("The stage's pods send the item's end within a renewal"
                + " interval: item.split once none of its failures is"
                + " open, else item.progress");
    }

    @Override
    public ReprocessStore reprocessRuns()
    {
        return new ReprocessStore(database.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public ReprocessStore reprocessRunsToChange()
    {
        return new ReprocessStore(writing.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    /**
     * The stage's input, as the split stage reprocesses it (STAGES.md
     * §15.8.4): each item once, at its whole-item place, under its current
     * generation. An item landed for the stage's intake but never split
     * (its message lost) is chosen at generation 1, to be split as its
     * next (§15.8.10 step 5); an item not landed there is chosen as
     * nothing.
     */
    @Override
    public ReprocessInputs reprocessInputs() throws Exception
    {
        ItemCheckpoints checkpoints = checkpoints();
        String intake = properties.landing().intake();
        return itemId ->
        {
            Integer generation = checkpoints.generations(List.of(itemId))
                    .get(itemId);
            if (generation == null)
            {
                boolean landed = LandedItems.summary(landedItems(), itemId)
                        .filter(summary -> intake.equals(summary.getField(
                                LandingTables.INTAKE)))
                        .isPresent();
                if (!landed)
                {
                    return List.of();
                }
                generation = 1;
            }
            return List.of(new ReprocessInputs.Input(
                    new ReprocessStore.Chosen(itemId,
                            FailureStore.WHOLE_ITEM,
                            Lineage.of(generation, List.of()),
                            itemId.toString()), false, null));
        };
    }

    /**
     * The split stage's version, as its checkpoints keep it: the build,
     * the splitter and the split settings (STAGES.md §14.2).
     */
    @Override
    public String version()
    {
        String format = properties.split().format();
        return ConnectorRuntime.version(SplitterRegistry.load(), format,
                ConnectorRuntime.splitConfig(properties.split(), format));
    }

    /** The one version a pod with these settings cuts under. */
    @Override
    public Collection<String> releaseVersions()
    {
        return List.of(version());
    }

    @Override
    public StageReleases stageReleases()
    {
        return new StageReleases(database.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public StageReleases stageReleasesToChange()
    {
        return new StageReleases(writing.dataSource(),
                properties.stage().registrySchema(), ItemProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    /** The items being split, archived or failed, as the pods find. */
    @Override
    public PartWay partWay() throws Exception
    {
        ItemCheckpoints checkpoints = checkpoints();
        return first -> items(checkpoints.partWay(first));
    }

    /** The items held at the split stage (STAGES.md §15.6d). */
    @Override
    public List<PartWay.Items> held(int first) throws Exception
    {
        return items(checkpoints().held(first));
    }

    /**
     * The checkpoints' items by version, as the drain check takes them.
     *
     * @param byVersion the items, by version
     * @return the same
     */
    private static List<PartWay.Items> items(
            List<ItemCheckpoints.ByVersion> byVersion)
    {
        return byVersion.stream().map(each -> new PartWay.Items(
                each.version(), each.count(), each.first())).toList();
    }

    @Override
    public boolean wholeItems()
    {
        return true;
    }

    /**
     * The items whose current generation was cut under a version, each at
     * its whole-item place.
     */
    @Override
    public Map<UUID, Set<Long>> madeBy(String version, List<UUID> itemIds,
            int limit) throws Exception
    {
        Map<UUID, Set<Long>> places = new LinkedHashMap<>();
        for (UUID itemId : checkpoints().madeBy(version, itemIds, limit))
        {
            places.put(itemId, Set.of(FailureStore.WHOLE_ITEM));
        }
        return places;
    }

    /**
     * The items landed in the range, from {@code landed_items}; the intake,
     * if given, must be the stage's own.
     */
    @Override
    public List<UUID> landedBetween(String intake, Instant from, Instant to,
            int limit) throws Exception
    {
        String own = properties.landing().intake();
        if (intake != null && !intake.equals(own))
        {
            throw new AdminArgs.UsageException("--intake must be the"
                    + " stage's own, " + own);
        }
        return LandedItems.landedBetween(landedItems(), own, from, to,
                limit);
    }

    /**
     * Operators' requests to release held items (STAGES.md §15.6e), to
     * record one: over the connection that may write.
     *
     * @return the store
     */
    HoldReleases releasesToChange()
    {
        return new HoldReleases(writing.dataSource(),
                properties.stage().registrySchema(), Duration.ofSeconds(10));
    }

    /**
     * Operators' requests to release held items, to list.
     *
     * @return the store
     */
    HoldReleases releases()
    {
        return new HoldReleases(database.dataSource(),
                properties.stage().registrySchema(), Duration.ofSeconds(10));
    }

    /**
     * The stage's item rows, without their checkpoints: needs no keys.
     *
     * @return the report
     */
    SplitItemReport items()
    {
        return new SplitItemReport(database.dataSource(),
                properties.stage().registrySchema());
    }

    /**
     * The leases, to inspect.
     *
     * @return the lease store
     */
    JdbcLeaseStore leases()
    {
        return database.leases();
    }

    @Override
    public void close()
    {
        if (keys != null)
        {
            keys.close();
        }
        if (landingCatalog != catalog)
        {
            closeQuietly(landingCatalog);
        }
        closeQuietly(catalog);
        database.close();
        writing.close();
    }

    /**
     * Closes a catalog, if it holds anything to close.
     *
     * @param catalog the catalog, or null
     */
    private static void closeQuietly(Object catalog)
    {
        if (catalog instanceof AutoCloseable closeable)
        {
            try
            {
                closeable.close();
            }
            catch (Exception ignored)
            {
                // exiting
            }
        }
    }
}
