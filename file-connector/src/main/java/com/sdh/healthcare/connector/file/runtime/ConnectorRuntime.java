/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.pipeline.PipelineSettings;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitLimits;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.split.ItemSplitter;
import com.sdh.healthcare.connector.file.split.SplitCorrections;
import com.sdh.healthcare.connector.file.split.LandedItemsTable;
import com.sdh.healthcare.connector.file.split.SplitHandler;
import com.sdh.healthcare.connector.file.split.SplitReconciler;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.stage.StageBuild;
import com.sdh.healthcare.connector.stage.StageDuties;
import com.sdh.healthcare.connector.stage.StageHealth;
import com.sdh.healthcare.connector.stage.StageProperties;
import com.sdh.healthcare.connector.stage.StageReceiver;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the split stage runs on (STAGES.md §8), started in a fixed
 * order: settings and keys → the lease database, holding the item and
 * duties leases and the items' checkpoints → the stage's catalog, with the
 * shared {@code split_records} table and its ID key → the landing catalog,
 * whose {@code landed_items} is read only, and loaded when first needed →
 * publisher, pipeline and splitter → the input route and the duties, both
 * left stopped. {@link #start()} starts them, and {@link #close()} stops
 * them first, so no item is being split when the rest is closed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConnectorRuntime implements AutoCloseable
{

    /**
     * The longest pause before a held batch is tried again. Kafka's consumer
     * doesn't poll while a batch is held, so the pause is kept well under
     * {@code max.poll.interval.ms} (5 minutes by default): a longer one took
     * the consumer out of its group at every retry (STAGES.md §10).
     */
    static final Duration MAX_BACKOFF = Duration.ofMinutes(1);

    /**
     * The longest a Kafka consumer may go without polling, unless its URI
     * sets {@code maxPollIntervalMs}: an item is split while its message is
     * held, and splitting a large one takes longer than Kafka's 5 minutes.
     * An item taking longer still is moved to another pod, which waits for
     * its lease, and the item is announced again (STAGES.md §10).
     */
    static final Duration MAX_POLL_INTERVAL = Duration.ofMinutes(30);

    /** The shortest age orphan cleanup may delete files at. */
    static final Duration MIN_ORPHAN_AGE = Duration.ofHours(1);

    /** Logs start and stop. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConnectorRuntime.class);

    /** The lease database's pool. */
    private final HikariDataSource dataSource;

    /** The stage's catalog. */
    private final Catalog catalog;

    /** The landing catalog; the stage's own if they are the same. */
    private final Catalog landingCatalog;

    /** Sends messages. */
    private final ClosablePublisher publisher;

    /** Splits, stores and publishes. */
    private final IngestPipeline pipeline;

    /** Takes items through the pipeline. */
    private final ItemProcessor processor;

    /** Splits one item at a time, under its lease. */
    private final ItemSplitter splitter;

    /** Takes {@code item.landed} messages off the broker. */
    /** Applies operators' corrections. */
    private final SplitCorrections corrections;

    private final StageReceiver receiver;

    /** Whether the broker answers. */
    private final StageHealth health;

    /** The reconciler and maintenance, on one pod. */
    private final StageDuties duties;

    /** The tables the runtime uses. */
    private final Tables tables;

    /** The keys, closed with the runtime. */
    private final KeyMaterial keys;

    /** Makes IDs and MACs with the stage's ID key. */
    private final RecordIds ids;

    /**
     * The tables the runtime uses.
     *
     * @param splitRecords the stage's {@code split_records}
     * @param landedItems  the landing node's {@code landed_items}, read
     *                     only
     * @param checkpoints  the items' checkpoints
     */
    public record Tables(Table splitRecords, LandedItemsTable landedItems,
            ItemCheckpoints checkpoints)
    {
    }

    /**
     * What a started runtime holds.
     *
     * @param dataSource     the lease database's pool
     * @param catalog        the stage's catalog
     * @param landingCatalog the landing catalog
     * @param publisher      sends messages
     * @param pipeline       splits, stores and publishes
     * @param processor      takes items through the pipeline
     * @param splitter       splits one item at a time
     * @param corrections    applies operators' corrections
     * @param receiver       takes messages off the broker
     * @param health         whether the broker answers
     * @param duties         the reconciler and maintenance
     * @param tables         the tables
     * @param keys           the keys
     * @param ids            makes IDs and MACs
     */
    private record Parts(HikariDataSource dataSource, Catalog catalog,
            Catalog landingCatalog, ClosablePublisher publisher,
            IngestPipeline pipeline, ItemProcessor processor,
            ItemSplitter splitter, SplitCorrections corrections,
            StageReceiver receiver, StageHealth health,
            StageDuties duties, Tables tables, KeyMaterial keys,
            RecordIds ids)
    {
    }

    /**
     * Holds what has been started.
     *
     * @param parts what has been started
     */
    private ConnectorRuntime(Parts parts)
    {
        this.dataSource = parts.dataSource();
        this.catalog = parts.catalog();
        this.landingCatalog = parts.landingCatalog();
        this.publisher = parts.publisher();
        this.pipeline = parts.pipeline();
        this.processor = parts.processor();
        this.splitter = parts.splitter();
        this.corrections = parts.corrections();
        this.receiver = parts.receiver();
        this.health = parts.health();
        this.duties = parts.duties();
        this.tables = parts.tables();
        this.keys = parts.keys();
        this.ids = parts.ids();
    }

    /**
     * Sets up the split stage's runtime, with its input and duties not yet
     * started.
     *
     * @param properties the stage's settings
     * @param camel      the Camel context, for the publisher and the input
     * @param fatal      stops the process on a fatal error
     * @return the runtime
     * @throws Exception if a setting is wrong, a key can't be loaded, or
     *                   storage can't be reached
     */
    public static ConnectorRuntime create(ConnectorProperties properties,
            CamelContext camel, FatalExit fatal) throws Exception
    {
        ConnectorProperties.Landing landing = properties.landing();
        String intake = require(landing.intake(), "sdh.landing.intake");
        String format = require(properties.split().format(),
                "sdh.split.format");
        SplitterRegistry splitters = SplitterRegistry.load();
        SplitConfig split = splitConfig(properties.split(), format);
        splitters.get(format).validate(split);
        Map<String, String> catalogProperties = require(
                properties.storage().catalog(), "sdh.storage.catalog");
        Map<String, String> landingCatalogProperties =
                landing.storage().catalog() == null
                        || landing.storage().catalog().isEmpty()
                        ? null : landing.storage().catalog();
        StageProperties stage = properties.stage();
        ReceiveSettings input = stage.receive(Set.of()).withKafkaDefault(
                "maxPollIntervalMs",
                Long.toString(MAX_POLL_INTERVAL.toMillis()));
        CommonSettings.Queue output = requireQueue(stage.output());
        QueueType queueType = QueueType.of(output.type());
        String queueUri = queueType.effectiveUri(output.uri());
        ConnectorProperties.Maintenance maintenance =
                properties.maintenance();
        MaintenanceSettings maintenanceSettings = maintenance.toSettings();
        if (maintenanceSettings.orphanMinAge().compareTo(MIN_ORPHAN_AGE) < 0)
        {
            throw new IllegalArgumentException(
                    "sdh.maintenance.orphans.min-age must be at least 1 hour");
        }
        Optional<Duration> retention = MaintenanceScheduler.retention(
                "sdh.maintenance.retention.records",
                maintenance.retention().records(), Duration.ofDays(1));
        KeyMaterial keys = KeyMaterial.load(properties.crypto());

        HikariDataSource dataSource = null;
        Catalog catalog = null;
        Catalog landingCatalog = null;
        ClosablePublisher publisher = null;
        IngestPipeline pipeline = null;
        StageReceiver receiver = null;
        try
        {
            dataSource = LeaseDatabase.dataSource(properties.lease().jdbc());
            JdbcLeaseStore leases = new JdbcLeaseStore(dataSource,
                    properties.lease().jdbc().table(),
                    Duration.ofSeconds(10));
            leases.createTableIfMissing();
            LeaseKeeper.Settings leaseSettings = leaseSettings(
                    properties.lease());

            String namespace = properties.storage().namespace();
            catalog = CatalogUtil.buildIcebergCatalog(
                    properties.storage().catalogName(), catalogProperties,
                    null);
            Table splitRecords = IngestTables.splitRecords(catalog,
                    namespace);
            String recordsKek = properties.crypto().keys().recordsKek();
            RecordIds ids = new RecordIds(IdKeys.loadOrCreateShared(
                    splitRecords, keys.wrap(), recordsKek));
            ItemCheckpoints checkpoints = new ItemCheckpoints(dataSource,
                    stage.registrySchema(), new CheckpointCodec(ids),
                    Duration.ofSeconds(10));
            checkpoints.createTableIfMissing();

            landingCatalog = landingCatalogProperties == null ? catalog
                    : CatalogUtil.buildIcebergCatalog(
                            landing.storage().catalogName(),
                            landingCatalogProperties, null);
            LandedItemsTable landedItems = new LandedItemsTable(
                    landingCatalog, landing.storage().namespace());

            RetryPolicy retry = new RetryPolicy(
                    properties.retry().maxAttempts(),
                    properties.retry().initialBackoff(),
                    properties.retry().maxBackoff());
            Clock clock = Clock.systemUTC();
            Publishers.configureBroker(camel, queueType,
                    output.artemis().brokerUrl(),
                    output.artemis().username(),
                    output.artemis().password());
            publisher = Publishers.create(camel, queueType, queueUri);
            pipeline = new IngestPipeline(
                    pipelineSettings(properties, format, split, retry)
                            .withVersion(version(splitters, format, split)),
                    splitRecords, checkpoints, keys.wrap(), ids, splitters,
                    new MessageSealer(keys.signer(), keys.signerKid(),
                            keys.recipient(), keys.recipientKid()),
                    publisher, clock);
            // The fail store's states, beside the checkpoints (STAGES.md
            // §15.3); the failed pieces stay in split_records.
            FailureStore failures = new FailureStore(dataSource,
                    stage.registrySchema(), ItemProcessor.STAGE,
                    Duration.ofSeconds(10));
            failures.createTableIfMissing();
            failures.gauges(Duration.ofMinutes(1));
            // Items ended before their ends were numbered (§15.6f).
            checkpoints.numberEarlierCompletions(stage.registrySchema(),
                    ItemProcessor.STAGE);
            // Operators' corrections, applied by any pod (§15.6d).
            CorrectionStore corrections = new CorrectionStore(dataSource,
                    stage.registrySchema(), ItemProcessor.STAGE,
                    Duration.ofSeconds(10));
            corrections.createTableIfMissing();
            // Operators' requests to release held items (§15.6e).
            HoldReleases releases = new HoldReleases(dataSource,
                    stage.registrySchema(), Duration.ofSeconds(10));
            releases.createTableIfMissing();
            ItemProcessor processor = new ItemProcessor(pipeline, intake,
                    failures, corrections);
            ItemSplitter splitter = new ItemSplitter(processor, landedItems,
                    keys.wrap(), leases, properties.connectorId(),
                    leaseSettings);
            SplitCorrections correcting = new SplitCorrections(corrections,
                    releases, splitter, leaseSettings.renewInterval(),
                    leaseSettings.ttl());

            StageHealth health = new StageHealth();
            receiver = new StageReceiver(camel, input,
                    new SplitHandler(splitter, keys.inbox(), intake,
                            landing.storage().catalogName(),
                            landing.storage().namespace(), fatal),
                    health, Fields.of(Attr.INTAKE, intake),
                    properties.retry().initialBackoff(), MAX_BACKOFF);

            ConnectorProperties.Reconcile reconcile = landing.reconcile();
            SplitReconciler reconciler = reconcile.enabled()
                    ? new SplitReconciler(checkpoints, splitter, landedItems,
                            intake, reconcile.grace(), clock)
                    : null;
            StageDuties duties = new StageDuties(leases, "split",
                    properties.connectorId(), leaseSettings,
                    reconciler == null ? null : reconciler::reconcile,
                    reconcile.interval(),
                    (committer, dutiesFatal) -> new MaintenanceScheduler(
                            committer, maintenanceSettings,
                            maintenance.toSchedule(),
                            () -> List.of(MaintenanceScheduler.Target.of(
                                    splitRecords, RecordsTable.INGESTED_AT,
                                    retention)),
                            new ReentrantLock(), dutiesFatal, clock),
                    fatal);
            Telemetry.get().audit("connector.started",
                    Fields.of(Attr.FORMAT, format).with(Attr.INTAKE, intake));
            return new ConnectorRuntime(new Parts(dataSource, catalog,
                    landingCatalog, publisher, pipeline, processor, splitter,
                    correcting, receiver, health, duties,
                    new Tables(splitRecords, landedItems, checkpoints), keys,
                    ids));
        }
        catch (Exception | Error failure)
        {
            closeQuietly(receiver);
            if (pipeline != null)
            {
                pipeline.close();
            }
            closeQuietly(publisher);
            if (landingCatalog != catalog)
            {
                closeQuietly(landingCatalog);
            }
            closeQuietly(catalog);
            closeQuietly(dataSource);
            keys.close();
            throw failure;
        }
    }

    /**
     * The lease timings, for the item and duties leases.
     *
     * @param settings the {@code sdh.lease} settings
     * @return the timings
     */
    private static LeaseKeeper.Settings leaseSettings(
            CommonSettings.Lease settings)
    {
        return new LeaseKeeper.Settings(settings.ttl(),
                settings.renewInterval(), settings.safetyMargin());
    }

    /**
     * The pipeline's settings, from the stage's.
     *
     * @param properties the stage's settings
     * @param format     the format
     * @param split      the checked split settings
     * @param retry      how transient failures are retried
     * @return the pipeline's settings
     */
    private static PipelineSettings pipelineSettings(
            ConnectorProperties properties, String format, SplitConfig split,
            RetryPolicy retry)
    {
        ConnectorProperties.Commit commit = properties.storage().commit();
        int workers = properties.encryptWorkers() > 0
                ? properties.encryptWorkers()
                : Runtime.getRuntime().availableProcessors();
        return PipelineSettings.defaults(properties.connectorId(), format,
                        properties.storage().catalogName(),
                        properties.crypto().keys().recordsKek())
                .withSplit(split)
                .withCommit(commit.maxRecords(), commit.maxBytes().toBytes(),
                        commit.maxInterval())
                .withWorkers(workers)
                .withRetry(retry);
    }

    /**
     * The split stage's version (STAGES.md §14.2): the build, the splitter
     * (its format and its own version) and a digest of the split settings,
     * such as {@code 0.1.0@2026-11-08T10:00:00Z/ccda/1/3f9a0c1d22b7}. Kept
     * with every record cut and in every item's checkpoint.
     *
     * @param splitters the splitters
     * @param format    the format split
     * @param split     the split settings
     * @return the version
     */
    static String version(SplitterRegistry splitters, String format,
            SplitConfig split)
    {
        return StageBuild.identity(ConnectorRuntime.class) + "/" + format
                + "/" + splitters.get(format).version() + "/"
                + split.digest();
    }

    /**
     * The split settings, with the format's defaults where none are set.
     *
     * @param settings the {@code sdh.split} settings
     * @param format   the format
     * @return the split settings
     */
    public static SplitConfig splitConfig(ConnectorProperties.Split settings,
            String format)
    {
        SplitConfig config = SplitConfig.defaults(format)
                .withBoundary(settings.boundary());
        if (settings.options() != null)
        {
            for (Map.Entry<String, String> option
                    : settings.options().entrySet())
            {
                config = config.withOption(option.getKey(), option.getValue());
            }
        }
        SplitLimits defaults = config.limits();
        ConnectorProperties.Limits limits = settings.limits();
        return config.withLimits(new SplitLimits(
                limits.maxItemBytes() != null
                        ? limits.maxItemBytes().toBytes()
                        : defaults.maxItemBytes(),
                limits.maxRecordBytes() != null
                        ? limits.maxRecordBytes().toBytes()
                        : defaults.maxRecordBytes(),
                limits.maxRecordsPerItem() != null
                        ? limits.maxRecordsPerItem()
                        : defaults.maxRecordsPerItem()));
    }

    /**
     * Starts the duties, then the input (design §5.3.6).
     *
     * @throws Exception if the input route can't start
     */
    public void start() throws Exception
    {
        duties.start();
        corrections.start();
        receiver.start();
        LOG.info("split.started");
    }

    /**
     * Takes items through the pipeline.
     *
     * @return the processor
     */
    public ItemProcessor processor()
    {
        return processor;
    }

    /**
     * Splits one item at a time, under its lease.
     *
     * @return the splitter
     */
    public ItemSplitter splitter()
    {
        return splitter;
    }

    /**
     * Applies operators' corrections.
     *
     * @return the loop
     */
    public SplitCorrections corrections()
    {
        return corrections;
    }

    /**
     * Whether the input broker answers.
     *
     * @return the health
     */
    public StageHealth health()
    {
        return health;
    }

    /**
     * The tables the runtime uses.
     *
     * @return the tables
     */
    public Tables tables()
    {
        return tables;
    }

    /**
     * Makes IDs and MACs with the stage's ID key.
     *
     * @return the IDs
     */
    public RecordIds ids()
    {
        return ids;
    }

    /**
     * The keys.
     *
     * @return the keys
     */
    public KeyMaterial keys()
    {
        return keys;
    }

    /**
     * Stops the input and the duties, then closes everything in reverse
     * order.
     */
    @Override
    public void close()
    {
        receiver.close();
        corrections.close();
        duties.close();
        pipeline.close();
        closeQuietly(publisher);
        keys.close();
        if (landingCatalog != catalog)
        {
            closeQuietly(landingCatalog);
        }
        closeQuietly(catalog);
        closeQuietly(dataSource);
        LOG.info("connector.stopped");
    }

    /**
     * Checks that a setting is there.
     *
     * @param <T>     its type
     * @param value   its value
     * @param setting its name, for the message
     * @return the value
     * @throws IllegalArgumentException if it is null, blank or empty
     */
    static <T> T require(T value, String setting)
    {
        if (value == null || value instanceof String text && text.isBlank()
                || value instanceof Map<?, ?> map && map.isEmpty())
        {
            throw new IllegalArgumentException(setting + " is required");
        }
        return value;
    }

    /**
     * Checks that the output queue is configured.
     *
     * @param queue the {@code sdh.stage.output} settings
     * @return the settings
     * @throws IllegalArgumentException if its type or URI is missing
     */
    private static CommonSettings.Queue requireQueue(
            CommonSettings.Queue queue)
    {
        if (queue == null)
        {
            throw new IllegalArgumentException(
                    StageProperties.PREFIX + ".output is required");
        }
        require(queue.type(), StageProperties.PREFIX + ".output.type");
        require(queue.uri(), StageProperties.PREFIX + ".output.uri");
        return queue;
    }

    /**
     * Closes something while shutting down, ignoring failures.
     *
     * @param resource the thing to close; may be null
     */
    private static void closeQuietly(Object resource)
    {
        if (resource instanceof AutoCloseable closeable)
        {
            try
            {
                closeable.close();
            }
            catch (Exception ignored)
            {
                // shutting down
            }
        }
    }
}
