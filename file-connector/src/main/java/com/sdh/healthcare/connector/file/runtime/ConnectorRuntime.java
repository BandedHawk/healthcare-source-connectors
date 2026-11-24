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
import com.sdh.healthcare.connector.contract.crypto.UnwrapCache;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.audit.AuditTables;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.HoldReleases;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.maintenance.ItemPurge;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.maintenance.Rekey;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.pipeline.PipelineSettings;
import com.sdh.healthcare.connector.core.pipeline.QueuedCommits;
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
import com.sdh.healthcare.connector.file.split.LandedItemsTable;
import com.sdh.healthcare.connector.file.split.SplitCorrections;
import com.sdh.healthcare.connector.file.split.SplitHandler;
import com.sdh.healthcare.connector.file.split.SplitReconciler;
import com.sdh.healthcare.connector.file.split.SplitReprocess;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.runtime.telemetry.AuditJournals;
import com.sdh.healthcare.connector.stage.ControlState;
import com.sdh.healthcare.connector.stage.DrainGate;
import com.sdh.healthcare.connector.stage.ItemEndResender;
import com.sdh.healthcare.connector.stage.KekWatch;
import com.sdh.healthcare.connector.stage.PartWay;
import com.sdh.healthcare.connector.stage.PauseWatch;
import com.sdh.healthcare.connector.stage.PurgePass;
import com.sdh.healthcare.connector.stage.PurgeRequests;
import com.sdh.healthcare.connector.stage.RekeyPass;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import com.sdh.healthcare.connector.stage.RetentionPolicy;
import com.sdh.healthcare.connector.stage.StageBuild;
import com.sdh.healthcare.connector.stage.StageDuties;
import com.sdh.healthcare.connector.stage.StageHealth;
import com.sdh.healthcare.connector.stage.StageKeys;
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

    /**
     * How many landing data keys the split stage keeps unwrapped: one per
     * landing group, so a few groups' worth.
     */
    static final int LANDING_KEYS_KEPT = 64;

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

    /** Sends reprocesses to the stage's own input; null if it doesn't. */
    private final ClosablePublisher reprocessPublisher;

    /** Splits, stores and publishes. */
    private final IngestPipeline pipeline;

    /** Takes items through the pipeline. */
    private final ItemProcessor processor;

    /** Splits items, each under its lease. */
    private final ItemSplitter splitter;

    /** Applies operators' corrections. */
    private final SplitCorrections corrections;

    /** The stage's reprocess; null if it doesn't reprocess. */
    private final SplitReprocess reprocess;

    /** Takes {@code item.landed} messages off the broker. */
    private final StageReceiver receiver;

    /** Whether the broker answers. */
    private final StageHealth health;

    /** The reconciler and maintenance, on one pod. */
    private final StageDuties duties;

    /** The tables the runtime uses. */
    private final Tables tables;

    /** The keys, closed with the runtime. */
    private final KeyMaterial keys;

    /** The audit journal (HIPAA.md §3.1), closed before the keys. */
    private final AuditJournals.Opened journal;

    /** Makes IDs and MACs with the stage's ID key. */
    private final RecordIds ids;

    /** Lets this pod's release in before it takes work (STAGES.md §14.4a). */
    private final DrainGate gate;

    /** Pauses the input and the corrections (CONTROL-API-DESIGN.md §9). */
    private final PauseWatch watch;

    /** The KEK new batches are wrapped with, as rotated (§11). */
    private final KekWatch kek;

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
     * @param reprocessing   sends reprocesses to the stage's own input;
     *                       null if it doesn't reprocess
     * @param pipeline       splits, stores and publishes
     * @param processor      takes items through the pipeline
     * @param splitter       splits items, each under its lease
     * @param corrections    applies operators' corrections
     * @param reprocess      the stage's reprocess; null if it doesn't
     * @param receiver       takes messages off the broker
     * @param health         whether the broker answers
     * @param duties         the reconciler and maintenance
     * @param tables         the tables
     * @param keys           the keys
     * @param journal        the audit journal
     * @param ids            makes IDs and MACs
     * @param gate           the drain check
     * @param watch          the pause's watch
     * @param kek            the KEK's watch
     */
    private record Parts(HikariDataSource dataSource, Catalog catalog,
            Catalog landingCatalog, ClosablePublisher publisher,
            ClosablePublisher reprocessing, IngestPipeline pipeline,
            ItemProcessor processor, ItemSplitter splitter,
            SplitCorrections corrections, SplitReprocess reprocess,
            StageReceiver receiver, StageHealth health, StageDuties duties,
            Tables tables, KeyMaterial keys, AuditJournals.Opened journal,
            RecordIds ids, DrainGate gate, PauseWatch watch, KekWatch kek)
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
        this.reprocessPublisher = parts.reprocessing();
        this.pipeline = parts.pipeline();
        this.processor = parts.processor();
        this.splitter = parts.splitter();
        this.corrections = parts.corrections();
        this.reprocess = parts.reprocess();
        this.receiver = parts.receiver();
        this.watch = parts.watch();
        this.kek = parts.kek();
        this.health = parts.health();
        this.duties = parts.duties();
        this.tables = parts.tables();
        this.keys = parts.keys();
        this.journal = parts.journal();
        this.ids = parts.ids();
        this.gate = parts.gate();
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
        if (stage.consumers() != 1)
        {
            // Split's consumers share its item limit (sdh.split.consumers).
            throw new IllegalArgumentException(StageProperties.PREFIX
                    + ".consumers is for a record stage; set"
                    + " sdh.split.consumers instead");
        }
        ReceiveSettings input = consumers(stage.receive(Set.of())
                .withKafkaDefault("maxPollIntervalMs",
                        Long.toString(MAX_POLL_INTERVAL.toMillis())),
                properties.split().consumers());
        CommonSettings.Queue output = requireQueue(stage.output());
        QueueType queueType = QueueType.of(output.type());
        String queueUri = queueType.effectiveUri(output.uri(),
                StageProperties.PREFIX + ".output.uri");
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
        // The setting's retention, until an operator sets another within
        // its bounds (CONTROL-API-DESIGN.md §11); a setting outside them
        // refuses startup.
        RetentionPolicy retentionPolicy = RetentionPolicy.of(
                retention.orElse(null), Duration.ofDays(1),
                maintenance.retentionBounds());
        retentionPolicy.check(retentionPolicy.setting());
        KeyMaterial keys = KeyMaterial.load(properties.crypto());

        HikariDataSource dataSource = null;
        AuditJournals.Opened journal = null;
        HikariDataSource commitPool = null;
        Catalog catalog = null;
        Catalog landingCatalog = null;
        ClosablePublisher publisher = null;
        ClosablePublisher reprocessPublisher = null;
        IngestPipeline pipeline = null;
        StageReceiver receiver = null;
        KekWatch kek = null;
        try
        {
            dataSource = LeaseDatabase.dataSource(properties.lease().jdbc(),
                    ItemProcessor.STAGE);
            // Every audit event, chained and sealed (HIPAA.md §3.1).
            journal = AuditJournals.start(properties.lease(),
                    properties.telemetry(),
                    new AuditTables(stage.registrySchema()),
                    ItemProcessor.STAGE + "/" + properties.connectorId(),
                    keys);
            // Statements asked for at once gathered into one (§18.3).
            int gather = properties.split().registryBatch();
            JdbcLeaseStore leases = new JdbcLeaseStore(dataSource,
                    properties.lease().jdbc().table(),
                    Duration.ofSeconds(10), gather);
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
            ControlState control = new ControlState(dataSource,
                    stage.registrySchema(), ItemProcessor.STAGE + "/"
                            + properties.connectorId());
            // An erased stage's pods never start again (§11).
            control.refuseIfErased();
            // The KEK each new batch is wrapped with, as rotated
            // (CONTROL-API-DESIGN.md §11), read before the first batch.
            kek = KekWatch.of(control, recordsKek, keys.wrap());
            kek.start();
            RecordIds ids = new RecordIds(IdKeys.loadOrCreateShared(
                    splitRecords, keys.wrap(), kek.get()));
            ItemCheckpoints checkpoints = new ItemCheckpoints(dataSource,
                    stage.registrySchema(), new CheckpointCodec(ids),
                    Duration.ofSeconds(10), gather);
            checkpoints.createTableIfMissing();
            DrainGate gate = DrainGate.open(dataSource, stage,
                    ItemProcessor.STAGE,
                    List.of(version(splitters, format, split)),
                    first -> checkpoints.partWay(first).stream()
                            .map(items -> new PartWay.Items(items.version(),
                                    items.count(), items.first()))
                            .toList());

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
            // Every pod's files committed together, a pod at a time, through
            // the stage database (docs/PERFORMANCE.md §3.7), on a pool of
            // its own: a pod waiting for its turn holds a connection.
            QueuedCommits queued = null;
            if (properties.split().commit().shared())
            {
                // A connection for each group waiting at once, and two
                // more: one to queue a group, one to settle an item.
                int groups = properties.split().commit().groupsAtOnce();
                commitPool = LeaseDatabase.dataSource(
                        properties.lease().jdbc(), "sdh-append", groups + 2);
                queued = new QueuedCommits(ItemProcessor.STAGE, commitPool,
                        stage.registrySchema(),
                        properties.lease().jdbc().table(),
                        leaseSettings.safetyMargin(),
                        properties.split().commit().interval(),
                        Duration.ofSeconds(10), groups);
                queued.createTablesIfMissing();
            }
            pipeline = new IngestPipeline(
                    pipelineSettings(properties, format, split, retry)
                            .withVersion(version(splitters, format, split)),
                    splitRecords, checkpoints, keys.wrap(), ids, splitters,
                    new MessageSealer(keys.messages(), keys.signerKid(),
                            keys.recipient(), keys.recipientKid()),
                    publisher, clock, queued);
            pipeline.setActiveKek(kek);
            commitPool = null;
            // The fail store's states, beside the checkpoints (STAGES.md
            // §15.3); the failed pieces stay in split_records.
            FailureStore failures = new FailureStore(dataSource,
                    stage.registrySchema(), ItemProcessor.STAGE,
                    Duration.ofSeconds(10));
            failures.createTableIfMissing();
            failures.gauges(Duration.ofMinutes(1));
            // Operators' corrections, applied by any pod (§15.6d); making
            // the table moves the fail store's states that went (§15.8.5).
            CorrectionStore corrections = new CorrectionStore(dataSource,
                    stage.registrySchema(), ItemProcessor.STAGE,
                    Duration.ofSeconds(10));
            corrections.createTableIfMissing();
            // Items ended before their ends were numbered (§15.6f).
            checkpoints.numberEarlierCompletions(stage.registrySchema(),
                    ItemProcessor.STAGE);
            // Operators' requests to release held items (§15.6e).
            HoldReleases releases = new HoldReleases(dataSource,
                    stage.registrySchema(), Duration.ofSeconds(10));
            releases.createTableIfMissing();
            // Operators' reprocess runs (§15.8.4), where the stage has its
            // own keys and input.
            ReprocessStore reprocessRuns = new ReprocessStore(dataSource,
                    stage.registrySchema(), ItemProcessor.STAGE,
                    Duration.ofSeconds(10));
            reprocessRuns.createTablesIfMissing();
            CommonSettings.Queue reprocessQueue = reprocessQueue(stage, keys);
            SplitReprocess reprocess = null;
            if (reprocessQueue != null)
            {
                reprocessPublisher = publisher(camel, reprocessQueue,
                        StageProperties.PREFIX + ".reprocess.uri");
                KeyMaterial.Self self = keys.self();
                reprocess = new SplitReprocess(new SplitReprocess.Parts(
                        reprocessRuns, new MessageSealer(keys.messages(),
                                keys.signerKid(), self.inbox(),
                                self.inboxKid()), reprocessPublisher,
                        self.signerKid()),
                        new SplitReprocess.Items(checkpoints, landedItems,
                                releases, landing.storage().catalogName(),
                                landing.storage().namespace(),
                                stage.sendTimeout(), leaseSettings.ttl()));
                pipeline.setInterruptions(reprocess::newer);
            }
            ItemProcessor processor = new ItemProcessor(pipeline, intake,
                    failures, corrections);
            // A landing group's items share a data key: one KMS call for
            // them all (docs/PERFORMANCE.md §10).
            ItemSplitter splitter = new ItemSplitter(processor, landedItems,
                    new UnwrapCache(keys.wrap(), LANDING_KEYS_KEPT), leases,
                    properties.connectorId(),
                    leaseSettings, properties.split().itemsAtOnce());
            SplitCorrections correcting = new SplitCorrections(corrections,
                    releases, splitter, reprocess,
                    leaseSettings.renewInterval(), leaseSettings.ttl());

            StageHealth health = new StageHealth();
            receiver = new StageReceiver(camel, input,
                    new SplitHandler(splitter, keys.inbox(), intake,
                            landing.storage().catalogName(),
                            landing.storage().namespace(), fatal,
                            reprocess),
                    health, Fields.of(Attr.INTAKE, intake),
                    properties.retry().initialBackoff(), MAX_BACKOFF);

            ConnectorProperties.Reconcile reconcile = landing.reconcile();
            SplitReconciler reconciler = reconcile.enabled()
                    ? new SplitReconciler(checkpoints, splitter, landedItems,
                            intake, reconcile.grace(), clock)
                    : null;
            // Ends the convert stage never received, sent again (§13.2).
            ItemEndResender resender = ItemEndResender.of(dataSource,
                    stage.nextRegistrySchema(), "split", splitter::resendEnd,
                    Duration.ofSeconds(10));
            HikariDataSource database = dataSource;
            StageDuties duties = new StageDuties(leases, "split",
                    properties.connectorId(), leaseSettings,
                    StageDuties.Reconciler.both(
                            reconciler == null ? null : reconciler::reconcile,
                            resender == null ? null : resender::resend),
                    reconcile.interval(),
                    (committer, dutiesFatal) -> new MaintenanceScheduler(
                            committer, maintenanceSettings,
                            maintenance.toSchedule(),
                            // The retention in force, read at each run.
                            () -> List.of(MaintenanceScheduler.Target.of(
                                    splitRecords, RecordsTable.INGESTED_AT,
                                    retentionPolicy.forRuns(control).get())),
                            new ReentrantLock(), dutiesFatal, clock,
                            // A re-key or purges asked for, carried out
                            // here.
                            MaintenanceScheduler.Pass.of(
                                    new RekeyPass(control, committer,
                                            keys.wrap(), database,
                                            ItemProcessor.STAGE,
                                            StageKeys.splitRows(
                                                    stage.registrySchema()),
                                            Rekey.BUDGET),
                                    new PurgePass(PurgeRequests.of(database,
                                            stage.registrySchema(), control),
                                            committer, database,
                                            CorrectionStore.table(
                                                    stage.registrySchema()),
                                            ItemProcessor.STAGE,
                                            ItemPurge.BUDGET))),
                    fatal).placed(stage.duties());
            PauseWatch watch = PauseWatch.of(control,
                    List.of(receiver, correcting));
            Telemetry.get().audit("connector.started",
                    Fields.of(Attr.FORMAT, format).with(Attr.INTAKE, intake));
            return new ConnectorRuntime(new Parts(dataSource, catalog,
                    landingCatalog, publisher, reprocessPublisher, pipeline,
                    processor, splitter, correcting, reprocess, receiver,
                    health, duties,
                    new Tables(splitRecords, landedItems, checkpoints), keys,
                    journal, ids, gate, watch, kek));
        }
        catch (Exception | Error failure)
        {
            closeQuietly(receiver);
            closeQuietly(kek);
            if (pipeline != null)
            {
                pipeline.close();
            }
            closeQuietly(reprocessPublisher);
            closeQuietly(publisher);
            if (landingCatalog != catalog)
            {
                closeQuietly(landingCatalog);
            }
            closeQuietly(catalog);
            closeQuietly(commitPool);
            closeQuietly(journal);
            closeQuietly(dataSource);
            keys.close();
            throw failure;
        }
    }

    /**
     * The split stage's input with as many Kafka consumers as
     * {@code sdh.split.consumers} asks (docs/PERFORMANCE.md §3.4), each
     * handling its own batches, so that one slow item holds back only its
     * own batch.
     *
     * @param input     the input as configured
     * @param consumers how many consumers
     * @return the input to receive from
     * @throws IllegalArgumentException if more than one is asked of a broker
     *                                  other than Kafka, or the URI sets
     *                                  {@code consumersCount} itself
     */
    static ReceiveSettings consumers(ReceiveSettings input, int consumers)
    {
        return input.withConsumers(consumers, "sdh.split.consumers");
    }

    /**
     * The stage's own input, if it reprocesses (STAGES.md §15.8.4): only
     * once its own keys ({@code sdh.crypto.keys.inbox.public-pem},
     * {@code sdh.crypto.keys.signing.public-pem}) and its own input
     * ({@code sdh.stage.reprocess}) are both configured.
     *
     * @param stage the stage's settings
     * @param keys  the stage's keys
     * @return the queue; null if it doesn't reprocess
     * @throws IllegalArgumentException if only one of the two is configured
     */
    static CommonSettings.Queue reprocessQueue(StageProperties stage,
            KeyMaterial keys)
    {
        CommonSettings.Queue queue = stage.reprocessQueue();
        KeyMaterial.Self self = keys.self();
        if (queue == null && self == null)
        {
            return null;
        }
        if (queue == null || self == null)
        {
            throw new IllegalArgumentException("A reprocess needs both "
                    + StageProperties.PREFIX + ".reprocess and the stage's"
                    + " own public keys (sdh.crypto.keys.inbox.public-pem,"
                    + " sdh.crypto.keys.signing.public-pem)");
        }
        return queue;
    }

    /**
     * Opens a publisher to a queue.
     *
     * @param camel the Camel context
     * @param queue   the queue's settings
     * @param setting its URI's setting, for messages
     * @return the publisher
     * @throws Exception if it can't be opened
     */
    private static ClosablePublisher publisher(CamelContext camel,
            CommonSettings.Queue queue, String setting) throws Exception
    {
        QueueType type = QueueType.of(queue.type());
        Publishers.configureBroker(camel, type, queue.artemis().brokerUrl(),
                queue.artemis().username(), queue.artemis().password());
        return Publishers.create(camel, type,
                type.effectiveUri(queue.uri(), setting));
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
    public static String version(SplitterRegistry splitters, String format,
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
     * Waits until the drain check lets this pod's release in (STAGES.md
     * §14.4a), then reads whether the stage is paused
     * (CONTROL-API-DESIGN.md §9), and starts the duties, the corrections
     * and the input (design §5.3.6), which a pause holds; a pod for the
     * duties alone
     * ({@code sdh.stage.duties.only}) starts only the duties. Nothing
     * starts if the runtime is closed while it waits.
     *
     * @throws Exception if the input route can't start
     */
    public void start() throws Exception
    {
        if (!gate.awaitLetIn())
        {
            LOG.info("split.not_started");
            return;
        }
        // Before any input: a paused stage takes none.
        watch.start();
        duties.start();
        if (duties.placement().only())
        {
            LOG.info("split.started duties_only=true");
            return;
        }
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
     * Splits items, each under its lease.
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
     * The stage's reprocess (STAGES.md §15.8.4).
     *
     * @return it; null if the stage doesn't reprocess
     */
    public SplitReprocess reprocess()
    {
        return reprocess;
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
        gate.close();
        watch.close();
        kek.close();
        receiver.close();
        corrections.close();
        duties.close();
        pipeline.close();
        closeQuietly(reprocessPublisher);
        closeQuietly(publisher);
        LOG.info("connector.stopped");
        journal.close();
        keys.close();
        if (landingCatalog != catalog)
        {
            closeQuietly(landingCatalog);
        }
        closeQuietly(catalog);
        closeQuietly(dataSource);
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
