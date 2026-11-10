/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record.runtime;

import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.record.BacklogWatch;
import com.sdh.healthcare.connector.record.RecordReconciler;
import com.sdh.healthcare.connector.record.RecordStageProcessor;
import com.sdh.healthcare.connector.record.RecordTable;
import com.sdh.healthcare.connector.record.RecordTransform;
import com.sdh.healthcare.connector.record.ResultReporter;
import com.sdh.healthcare.connector.record.SourceInputs;
import com.sdh.healthcare.connector.record.config.RecordStageSettings;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.sdh.healthcare.connector.stage.DrainGate;
import com.sdh.healthcare.connector.stage.ReprocessStore;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything a record stage runs on (STAGES.md §17), set up in a fixed
 * order: settings and keys → the lease database, holding the duties lease
 * and the stage's registry → the source and sink, with the sink's ID key →
 * the reader, publishers and results channel → the worker → the input
 * route and the duties, both left stopped. {@link #start()} starts them,
 * and {@link #close()} stops them first, so no record is being made when
 * the rest is closed. The stage's own step is its {@link RecordTransform};
 * everything else is chosen by settings.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordStageRuntime implements AutoCloseable
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

    /** The share of the heap transforms may use, unless set. */
    static final double DEFAULT_MEMORY_SHARE = 0.4;

    /** A resource that takes this long to close is logged. */
    static final long SLOW_CLOSE_MILLIS = 100;

    /** How long a registry or lease call may wait for a connection. */
    static final Duration DATABASE_WAIT = Duration.ofSeconds(10);

    /** Logs start and stop. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RecordStageRuntime.class);

    /** The stage's name, in logs. */
    private final String stage;

    /** What the runtime holds, closed in reverse order. */
    private final List<AutoCloseable> resources;

    /** Claims, transforms, stores and sends records. */
    private final StageWorker<RecordStageProcessor.Prepared> worker;

    /** Takes the stage before's messages off the broker. */
    private final StageReceiver receiver;

    /** Whether the broker answers. */
    private final StageHealth health;

    /** The reconciler and maintenance, on one pod. */
    private final StageDuties duties;

    /** The source table. */
    private final RecordTable source;

    /** The sink table. */
    private final RecordTable sink;

    /** Lets this pod's release in before it takes work (STAGES.md §14.4a). */
    private final DrainGate gate;

    /**
     * What a set-up runtime holds.
     *
     * @param stage     the stage's name
     * @param resources what to close, in the order opened
     * @param worker    the worker
     * @param receiver  the input
     * @param health    whether the broker answers
     * @param duties    the reconciler and maintenance
     * @param source    the source table
     * @param sink      the sink table
     * @param gate      the drain check
     */
    private record Parts(String stage, List<AutoCloseable> resources,
            StageWorker<RecordStageProcessor.Prepared> worker,
            StageReceiver receiver, StageHealth health, StageDuties duties,
            RecordTable source, RecordTable sink, DrainGate gate)
    {
    }

    /**
     * Holds what has been set up.
     *
     * @param parts what has been set up
     */
    private RecordStageRuntime(Parts parts)
    {
        this.stage = parts.stage();
        this.resources = parts.resources();
        this.worker = parts.worker();
        this.receiver = parts.receiver();
        this.health = parts.health();
        this.duties = parts.duties();
        this.source = parts.source();
        this.sink = parts.sink();
        this.gate = parts.gate();
    }

    /**
     * Sets up a record stage's runtime, with its input and duties not yet
     * started.
     *
     * @param settings  the stage's settings
     * @param transform the stage's own step
     * @param camel     the Camel context, for the publishers and the input
     * @param fatal     stops the process on a fatal error
     * @return the runtime
     * @throws Exception if a setting is wrong, a key can't be loaded, or
     *                   storage can't be reached
     */
    public static RecordStageRuntime create(RecordStageSettings settings,
            RecordTransform transform, CamelContext camel, FatalExit fatal)
            throws Exception
    {
        String stageName = transform.messages().stage();
        StageProperties stage = settings.stage();
        StageWorker.Settings workerSettings = stage.worker();
        CommonSettings.Queue output = stage.output();
        RecordStageSettings.Maintenance maintenance = settings.maintenance();
        MaintenanceSettings maintenanceSettings = maintenance.toSettings();
        if (maintenanceSettings.orphanMinAge().compareTo(MIN_ORPHAN_AGE) < 0)
        {
            throw new IllegalArgumentException(
                    "sdh.maintenance.orphans.min-age must be at least 1 hour");
        }
        Optional<Duration> retention = MaintenanceScheduler.retention(
                "sdh.maintenance.retention", maintenance.retention(),
                Duration.ofDays(1));
        String sinkKek = StageTables.sinkKek(settings);
        long memory = memoryBytes(settings);
        KeyMaterial keys = KeyMaterial.forStage(settings.crypto(),
                KeyMaterial.Providers.DEFAULT);

        List<AutoCloseable> opened = new ArrayList<>();
        opened.add(keys);
        try
        {
            HikariDataSource dataSource =
                    LeaseDatabase.dataSource(settings.lease().jdbc());
            opened.add(dataSource);
            JdbcLeaseStore leases = new JdbcLeaseStore(dataSource,
                    settings.lease().jdbc().table(), DATABASE_WAIT);
            leases.createTableIfMissing();
            StageRegistry registry = new StageRegistry(dataSource,
                    stage.registrySchema(), stageName,
                    LeaseKeeper.defaultOwner(), DATABASE_WAIT);
            registry.createTablesIfMissing();
            // Its open failures, for alerting (STAGES.md §15.6).
            registry.failureStore().gauges(Duration.ofMinutes(1));
            DrainGate gate = DrainGate.open(dataSource, stage, stageName,
                    transform.versions(), registry::partWay);

            StageTables tables = new StageTables(settings,
                    transform.messages());
            opened.add(tables);
            RecordTable source = tables.source();
            RecordTable sink = tables.sink();
            RecordIds ids = new RecordIds(IdKeys.loadOrCreateShared(
                    sink.table(), keys.wrap(), sinkKek));
            QueueReader reader = QueueReader.builder()
                    .catalog(tables.sourceCatalog())
                    .expectedCatalogName(source.catalogName())
                    .keyProvider(keys.wrap())
                    .build();
            opened.add(reader);

            RetryPolicy retry = new RetryPolicy(
                    settings.retry().maxAttempts(),
                    settings.retry().initialBackoff(),
                    settings.retry().maxBackoff());
            Clock clock = Clock.systemUTC();
            ClosablePublisher publisher = publisher(camel, output);
            opened.add(publisher);
            ResultReporter results = null;
            List<RecordStageProcessor.Earlier> earlier = List.of();
            if (settings.results().enabled())
            {
                ResultsChannel channel = ResultsChannel.open(
                        settings.results().channel(), output, camel,
                        keys.signer(), keys.signerKid(), retry,
                        RESULT_TIMEOUT);
                opened.add(channel);
                results = new ResultReporter(channel, clock);
                earlier = tables.earlier(settings.results().upstream());
            }

            RecordStageSettings.Reconcile reconcile = settings.reconcile();
            // Its own instance of the table: the reconciler refreshes its.
            BacklogWatch backlog = reconcile.enabled()
                    && reconcile.backlogQuiet().isPositive()
                    ? new BacklogWatch(tables.source().table(), registry,
                            reconcile.grace(), clock)
                    : null;
            RecordStageProcessor processor = new RecordStageProcessor(
                    transform, reader, new RecordStageProcessor.Tables(
                            source, new RecordStageProcessor.Sink(sink,
                                    keys.wrap(), sinkKek, ids), earlier),
                    memory, results, clock, backlog);
            StageWorker.Reprocessing reprocessing = reprocessing(camel,
                    stage, keys, new ReprocessStore(dataSource,
                            stage.registrySchema(), stageName, DATABASE_WAIT),
                    new SourceInputs(source, transform.messages()), opened);
            StageWorker<RecordStageProcessor.Prepared> worker =
                    new StageWorker<>(processor, registry, keys.inbox(),
                            new MessageSealer(keys.signer(), keys.signerKid(),
                                    keys.recipient(), keys.recipientKid()),
                            publisher, workerSettings, reprocessing);
            opened.add(worker);
            StageHealth health = new StageHealth();
            StageReceiver receiver = new StageReceiver(camel,
                    stage.receive(Set.of()), worker, health,
                    Fields.of(Attr.STAGE, stageName),
                    settings.retry().initialBackoff(), MAX_BACKOFF);
            opened.add(receiver);

            RecordReconciler reconciler = reconcile.enabled()
                    ? new RecordReconciler(source, transform.messages(),
                            registry, worker, processor, reconcile.grace(),
                            reconcile.lookback(), clock,
                            new RecordReconciler.Deferral(
                                    reconcile.backlogQuiet(),
                                    reconcile.maxDeferral()))
                    : null;
            CommonSettings.Lease lease = settings.lease();
            StageDuties duties = new StageDuties(leases, stageName,
                    settings.connectorId(),
                    new LeaseKeeper.Settings(lease.ttl(),
                            lease.renewInterval(), lease.safetyMargin()),
                    reconciler == null ? null : reconciler::reconcile,
                    reconcile.interval(),
                    (committer, dutiesFatal) -> new MaintenanceScheduler(
                            committer, maintenanceSettings,
                            maintenance.toSchedule(),
                            () -> List.of(MaintenanceScheduler.Target.of(
                                    sink.table(), RecordsTable.INGESTED_AT,
                                    retention)),
                            new ReentrantLock(), dutiesFatal, clock),
                    fatal);
            opened.add(duties);
            // Last, so closed first: shutting down stops a held pod's wait.
            opened.add(gate);
            Telemetry.get().audit("connector.started",
                    Fields.of(Attr.STAGE, stageName));
            return new RecordStageRuntime(new Parts(stageName,
                    List.copyOf(opened), worker, receiver, health, duties,
                    source, sink, gate));
        }
        catch (Exception | Error failure)
        {
            closeAll(opened);
            throw failure;
        }
    }

    /**
     * The heap the transforms running at once may use: the stage's
     * setting, or a share of the heap.
     *
     * @param settings the stage's settings
     * @return the bytes
     */
    static long memoryBytes(RecordStageSettings settings)
    {
        long set = settings.memoryBytes();
        return set > 0 ? set
                : (long) (Runtime.getRuntime().maxMemory()
                        * DEFAULT_MEMORY_SHARE);
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
            SourceInputs inputs, List<AutoCloseable> opened)
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
            LOG.info("stage.not_started stage={}", stage);
            return;
        }
        worker.start();
        duties.start();
        receiver.start();
        LOG.info("stage.started stage={}", stage);
    }

    /**
     * The worker, for tests and the reconciler.
     *
     * @return the worker
     */
    public StageWorker<RecordStageProcessor.Prepared> worker()
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
     * The source table.
     *
     * @return the table
     */
    public RecordTable source()
    {
        return source;
    }

    /**
     * The sink table.
     *
     * @return the table
     */
    public RecordTable sink()
    {
        return sink;
    }

    /**
     * Stops the input, the duties and the worker, then closes the rest in
     * reverse order.
     */
    @Override
    public void close()
    {
        LOG.info("stage.stopping stage={}", stage);
        long start = System.nanoTime();
        closeAll(resources);
        LOG.info("stage.stopped stage={} elapsed_ms={}", stage,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
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
                LOG.info("stage.slow_close resource={} elapsed_ms={}",
                        resource.getClass().getSimpleName(), elapsed);
            }
        }
    }
}
