/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.runtime;

import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.convert.BacklogWatch;
import com.sdh.healthcare.connector.convert.ConvertInputs;
import com.sdh.healthcare.connector.convert.ConvertProcessor;
import com.sdh.healthcare.connector.convert.ConvertReconciler;
import com.sdh.healthcare.connector.convert.ResultReporter;
import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.sdh.healthcare.connector.stage.DrainGate;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import com.sdh.healthcare.connector.stage.StageBuild;
import com.sdh.healthcare.connector.stage.StageDuties;
import com.sdh.healthcare.connector.stage.StageHealth;
import com.sdh.healthcare.connector.stage.StageProperties;
import com.sdh.healthcare.connector.stage.StageReceiver;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.StageWorker;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the convert stage runs on (STAGES.md §9), set up in a fixed
 * order: settings and keys → the lease database, holding the duties lease
 * and the stage's registry → the catalog, with the shared {@code records}
 * table and its ID key, and {@code split_records}, read only → the
 * reader, publishers and results channel → the worker → the input route
 * and the duties, both left stopped. {@link #start()} starts them, and
 * {@link #close()} stops them first, so no record is being converted when
 * the rest is closed.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertRuntime implements AutoCloseable
{

    /**
     * The longest pause before a held batch is tried again. Kafka's consumer
     * doesn't poll while a batch is held, so the pause is kept well under
     * {@code max.poll.interval.ms} (5 minutes by default): a longer one took
     * the consumer out of its group at every retry (STAGES.md §10).
     */
    static final Duration MAX_BACKOFF = Duration.ofMinutes(1);

    /** The shortest age orphan cleanup may delete files at. */
    static final Duration MIN_ORPHAN_AGE = Duration.ofHours(1);

    /** How long to wait for the producer's broker to take a result. */
    static final Duration RESULT_TIMEOUT = Duration.ofSeconds(30);

    /** The share of the heap conversions may use, unless set. */
    static final double DEFAULT_MEMORY_SHARE = 0.4;

    /** A resource that takes this long to close is logged. */
    static final long SLOW_CLOSE_MILLIS = 100;

    /** Logs start and stop. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConvertRuntime.class);

    /** What the runtime holds, closed in reverse order. */
    private final List<AutoCloseable> resources;

    /** Claims, converts, stores and sends records. */
    private final StageWorker<ConvertProcessor.Prepared> worker;

    /** Takes {@code record.split} and {@code item.split} off the broker. */
    private final StageReceiver receiver;

    /** Whether the broker answers. */
    private final StageHealth health;

    /** The reconciler and maintenance, on one pod. */
    private final StageDuties duties;

    /** The shared {@code records} table. */
    private final Table records;

    /** Lets this pod's release in before it takes work (STAGES.md §14.4a). */
    private final DrainGate gate;

    /**
     * What a set-up runtime holds.
     *
     * @param resources what to close, in the order opened
     * @param worker    the worker
     * @param receiver  the input
     * @param health    whether the broker answers
     * @param duties    the reconciler and maintenance
     * @param records   the {@code records} table
     * @param gate      the drain check
     */
    private record Parts(List<AutoCloseable> resources,
            StageWorker<ConvertProcessor.Prepared> worker,
            StageReceiver receiver, StageHealth health, StageDuties duties,
            Table records, DrainGate gate)
    {
    }

    /**
     * Holds what has been set up.
     *
     * @param parts what has been set up
     */
    private ConvertRuntime(Parts parts)
    {
        this.resources = parts.resources();
        this.worker = parts.worker();
        this.receiver = parts.receiver();
        this.health = parts.health();
        this.duties = parts.duties();
        this.records = parts.records();
        this.gate = parts.gate();
    }

    /**
     * Sets up the convert stage's runtime, with its input and duties not
     * yet started.
     *
     * @param properties the stage's settings
     * @param camel      the Camel context, for the publishers and the input
     * @param fatal      stops the process on a fatal error
     * @return the runtime
     * @throws Exception if a setting is wrong, a key can't be loaded, or
     *                   storage can't be reached
     */
    public static ConvertRuntime create(ConvertProperties properties,
            CamelContext camel, FatalExit fatal) throws Exception
    {
        ConvertProperties.Storage storageSettings = properties.storage();
        Map<String, String> catalogProperties = require(
                storageSettings.catalog(), "sdh.storage.catalog");
        StageProperties stage = properties.stage();
        StageWorker.Settings workerSettings = stage.worker();
        CommonSettings.Queue output = stage.output();
        ConvertProperties.Maintenance maintenance = properties.maintenance();
        MaintenanceSettings maintenanceSettings = maintenance.toSettings();
        if (maintenanceSettings.orphanMinAge().compareTo(MIN_ORPHAN_AGE) < 0)
        {
            throw new IllegalArgumentException(
                    "sdh.maintenance.orphans.min-age must be at least 1 hour");
        }
        Optional<Duration> retention = MaintenanceScheduler.retention(
                "sdh.maintenance.retention", maintenance.retention(),
                Duration.ofDays(1));
        ConvertProcessor.Settings convertSettings =
                convertSettings(properties);
        String recordsKek = require(properties.crypto().keys().recordsKek(),
                "sdh.crypto.keys.records-kek");
        KeyMaterial keys = KeyMaterial.forStage(properties.crypto(),
                KeyMaterial.Providers.DEFAULT);

        List<AutoCloseable> opened = new ArrayList<>();
        opened.add(keys);
        try
        {
            HikariDataSource dataSource =
                    LeaseDatabase.dataSource(properties.lease().jdbc());
            opened.add(dataSource);
            JdbcLeaseStore leases = new JdbcLeaseStore(dataSource,
                    properties.lease().jdbc().table(),
                    Duration.ofSeconds(10));
            leases.createTableIfMissing();
            StageRegistry registry = new StageRegistry(dataSource,
                    stage.registrySchema(), ConvertProcessor.STAGE,
                    LeaseKeeper.defaultOwner(), Duration.ofSeconds(10));
            registry.createTablesIfMissing();
            // Its open failures, for alerting (STAGES.md §15.6).
            registry.failureStore().gauges(Duration.ofMinutes(1));
            ConverterRegistry converters = ConverterRegistry.load();
            DrainGate gate = DrainGate.open(dataSource, stage,
                    ConvertProcessor.STAGE,
                    ConvertProcessor.versions(converters, convertSettings),
                    registry::partWay);

            Catalog catalog = CatalogUtil.buildIcebergCatalog(
                    storageSettings.catalogName(), catalogProperties, null);
            if (catalog instanceof AutoCloseable closeable)
            {
                opened.add(closeable);
            }
            Table records = IngestTables.sharedRecords(catalog,
                    storageSettings.namespace());
            Table splitRecords = IngestTables.splitRecords(catalog,
                    storageSettings.effectiveSplitNamespace());
            RecordIds ids = new RecordIds(IdKeys.loadOrCreateShared(records,
                    keys.wrap(), recordsKek));
            QueueReader reader = QueueReader.builder()
                    .catalog(catalog)
                    .expectedCatalogName(storageSettings.catalogName())
                    .keyProvider(keys.wrap())
                    .build();
            opened.add(reader);

            RetryPolicy retry = new RetryPolicy(
                    properties.retry().maxAttempts(),
                    properties.retry().initialBackoff(),
                    properties.retry().maxBackoff());
            Clock clock = Clock.systemUTC();
            ClosablePublisher publisher = publisher(camel, output);
            opened.add(publisher);
            ResultReporter results = null;
            if (properties.results().enabled())
            {
                ResultsChannel channel = ResultsChannel.open(
                        properties.results(), output, camel, keys.signer(),
                        keys.signerKid(), retry, RESULT_TIMEOUT);
                opened.add(channel);
                results = new ResultReporter(channel, clock);
            }

            ConvertProperties.Reconcile reconcile = properties.reconcile();
            // Its own instance of the table: the reconciler refreshes its.
            BacklogWatch backlog = reconcile.enabled()
                    && reconcile.backlogQuiet().isPositive()
                    ? new BacklogWatch(IngestTables.splitRecords(catalog,
                            storageSettings.effectiveSplitNamespace()),
                            registry, reconcile.grace(), clock)
                    : null;
            ConvertProcessor processor = new ConvertProcessor(reader,
                    converters, convertSettings,
                    new ConvertProcessor.Storage(records, keys.wrap(),
                            recordsKek, ids, splitRecords), results, clock,
                    backlog);
            StageWorker.Reprocessing reprocessing = reprocessing(camel,
                    stage, keys, new ReprocessStore(dataSource,
                            stage.registrySchema(), ConvertProcessor.STAGE,
                            Duration.ofSeconds(10)),
                    new ConvertInputs(splitRecords,
                            storageSettings.catalogName(),
                            storageSettings.effectiveSplitNamespace()),
                    opened);
            StageWorker<ConvertProcessor.Prepared> worker =
                    new StageWorker<>(processor, registry, keys.inbox(),
                            new MessageSealer(keys.signer(), keys.signerKid(),
                                    keys.recipient(), keys.recipientKid()),
                            publisher, workerSettings, reprocessing);
            opened.add(worker);
            StageHealth health = new StageHealth();
            StageReceiver receiver = new StageReceiver(camel,
                    stage.receive(Set.of()), worker, health,
                    Fields.of(Attr.STAGE, ConvertProcessor.STAGE),
                    properties.retry().initialBackoff(), MAX_BACKOFF);
            opened.add(receiver);

            ConvertReconciler reconciler = reconcile.enabled()
                    ? new ConvertReconciler(splitRecords, registry, worker,
                            processor, storageSettings.catalogName(),
                            storageSettings.effectiveSplitNamespace(),
                            reconcile.grace(), reconcile.lookback(), clock,
                            new ConvertReconciler.Deferral(
                                    reconcile.backlogQuiet(),
                                    reconcile.maxDeferral()))
                    : null;
            CommonSettings.Lease lease = properties.lease();
            StageDuties duties = new StageDuties(leases,
                    ConvertProcessor.STAGE, properties.connectorId(),
                    new LeaseKeeper.Settings(lease.ttl(),
                            lease.renewInterval(), lease.safetyMargin()),
                    reconciler == null ? null : reconciler::reconcile,
                    reconcile.interval(),
                    (committer, dutiesFatal) -> new MaintenanceScheduler(
                            committer, maintenanceSettings,
                            maintenance.toSchedule(),
                            () -> List.of(MaintenanceScheduler.Target.of(
                                    records, RecordsTable.INGESTED_AT,
                                    retention)),
                            new ReentrantLock(), dutiesFatal, clock),
                    fatal);
            opened.add(duties);
            // Last, so closed first: shutting down stops a held pod's wait.
            opened.add(gate);
            Telemetry.get().audit("connector.started",
                    Fields.of(Attr.STAGE, ConvertProcessor.STAGE));
            return new ConvertRuntime(new Parts(List.copyOf(opened), worker,
                    receiver, health, duties, records, gate));
        }
        catch (Exception | Error failure)
        {
            closeAll(opened);
            throw failure;
        }
    }

    /**
     * The conversion settings, from the stage's.
     *
     * @param properties the stage's settings
     * @return the settings
     * @throws IllegalArgumentException if the largest record is not
     *                                  positive
     */
    public static ConvertProcessor.Settings convertSettings(
            ConvertProperties properties)
    {
        ConvertProperties.Convert convert = properties.convert();
        long maxBytes = convert.maxBytes().toBytes();
        if (maxBytes < 1)
        {
            throw new IllegalArgumentException(
                    "sdh.convert.max-bytes must be positive");
        }
        long memory = convert.memory() != null
                && convert.memory().toBytes() > 0
                ? convert.memory().toBytes()
                : (long) (Runtime.getRuntime().maxMemory()
                        * DEFAULT_MEMORY_SHARE);
        return new ConvertProcessor.Settings(convert.options(), maxBytes,
                memory, properties.storage().catalogName(),
                properties.storage().namespace(),
                StageBuild.identity(ConvertRuntime.class));
    }

    /**
     * How the stage reprocesses (STAGES.md §15.8.4): only once its own
     * keys ({@code sdh.crypto.keys.inbox.public-pem},
     * {@code sdh.crypto.keys.signing.public-pem}) and its own input
     * ({@code sdh.stage.reprocess}) are both configured.
     *
     * @param camel  the Camel context
     * @param stage  the stage's settings
     * @param keys   the stage's keys
     * @param store  its runs
     * @param inputs its input
     * @param opened what to close with the runtime; the publisher is added
     * @return how it reprocesses; null if it doesn't
     * @throws IllegalArgumentException if only one of the two is configured
     * @throws Exception                if the publisher can't be started
     */
    private static StageWorker.Reprocessing reprocessing(CamelContext camel,
            StageProperties stage, KeyMaterial keys, ReprocessStore store,
            ConvertInputs inputs, List<AutoCloseable> opened)
            throws Exception
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
        ClosablePublisher publisher = publisher(camel, queue);
        opened.add(publisher);
        return new StageWorker.Reprocessing(store, inputs,
                new MessageSealer(keys.signer(), keys.signerKid(),
                        self.inbox(), self.inboxKid()),
                publisher, self.signerKid());
    }

    /**
     * Opens a publisher to a queue.
     *
     * @param camel the Camel context
     * @param queue the queue's settings
     * @return the publisher
     * @throws Exception if it can't be opened
     */
    private static ClosablePublisher publisher(CamelContext camel,
            CommonSettings.Queue queue) throws Exception
    {
        QueueType type = QueueType.of(queue.type());
        Publishers.configureBroker(camel, type, queue.artemis().brokerUrl(),
                queue.artemis().username(), queue.artemis().password());
        return Publishers.create(camel, type, type.effectiveUri(queue.uri()));
    }

    /**
     * Waits until the drain check lets this pod's release in (STAGES.md
     * §14.4a), then starts the worker's claim renewal, the duties and the
     * input. Nothing starts if the runtime is closed while it waits.
     *
     * @throws Exception if the input route can't start
     */
    public void start() throws Exception
    {
        if (!gate.awaitLetIn())
        {
            LOG.info("convert.not_started");
            return;
        }
        worker.start();
        duties.start();
        receiver.start();
        LOG.info("convert.started");
    }

    /**
     * The worker, for tests and the reconciler.
     *
     * @return the worker
     */
    public StageWorker<ConvertProcessor.Prepared> worker()
    {
        return worker;
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
     * The shared {@code records} table.
     *
     * @return the table
     */
    public Table records()
    {
        return records;
    }

    /**
     * Stops the input, the duties and the worker, then closes the rest in
     * reverse order.
     */
    @Override
    public void close()
    {
        LOG.info("convert.stopping");
        long start = System.nanoTime();
        closeAll(resources);
        LOG.info("convert.stopped elapsed_ms={}", TimeUnit.NANOSECONDS
                .toMillis(System.nanoTime() - start));
    }

    /**
     * Closes resources in reverse order, ignoring failures: shutting down.
     *
     * @param opened what was opened, in order
     */
    private static void closeAll(List<AutoCloseable> opened)
    {
        List<AutoCloseable> reversed = new ArrayList<>(opened);
        Collections.reverse(reversed);
        for (AutoCloseable resource : reversed)
        {
            long start = System.nanoTime();
            try
            {
                resource.close();
            }
            catch (Exception ignored)
            {
                // shutting down
            }
            long elapsed = TimeUnit.NANOSECONDS
                    .toMillis(System.nanoTime() - start);
            if (elapsed >= SLOW_CLOSE_MILLIS)
            {
                LOG.info("convert.slow_close resource={} elapsed_ms={}",
                        resource.getClass().getSimpleName(), elapsed);
            }
        }
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
}
