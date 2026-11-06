/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.inbox.Inbox;
import com.sdh.healthcare.connector.core.inbox.InboxTables;
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
import com.sdh.healthcare.connector.file.inbox.InboxReceiver;
import com.sdh.healthcare.connector.file.inbox.InboxRoute;
import com.sdh.healthcare.connector.file.inbox.LandedItemsTable;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the splitter runs on, started in a fixed order (design §9.3,
 * §5.3.6): settings and keys → lease → the splitter's catalog, with the
 * {@code records} and {@code inbox} tables fenced with the lease's epoch
 * → ID key → the landing catalog, whose {@code landed_items} is read
 * only, and loaded when first needed →
 * pipeline, processor and inbox → publisher. Closed in reverse, after the
 * inbox route has stopped, so no message is in flight when the lease is
 * released.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConnectorRuntime implements AutoCloseable
{

    /** How long to wait for the broker to acknowledge a result. */
    static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    /** Logs start and stop. */
    private static final Logger LOG =
            LoggerFactory.getLogger(ConnectorRuntime.class);

    /** Stops the route on close. */
    private final CamelContext camel;

    /** The lease database's pool. */
    private final HikariDataSource dataSource;

    /** The splitter's catalog. */
    private final Catalog catalog;

    /** The landing catalog; the splitter's own if they are the same. */
    private final Catalog landingCatalog;

    /** The splitter's lease. */
    private final LeaseKeeper lease;

    /** Sends messages. */
    private final ClosablePublisher publisher;

    /** Splits, stores and publishes. */
    private final IngestPipeline pipeline;

    /** Takes items through the pipeline. */
    private final ItemProcessor processor;

    /** Where items wait. */
    private final Inbox inbox;

    /** Takes {@code item.landed} messages into the inbox. */
    private final InboxReceiver receiver;

    /** The tables the runtime uses. */
    private final Tables tables;

    /** The keys, closed with the runtime. */
    private final KeyMaterial keys;

    /** Makes IDs and MACs with the splitter's ID key. */
    private final RecordIds ids;

    /** Makes fenced commits under the lease. */
    private final FencedCommitter committer;

    /** How transient failures are retried. */
    private final RetryPolicy retry;

    /** Sends final results to the producer; null if they are off. */
    private final ResultsChannel results;

    /**
     * The tables the runtime uses.
     *
     * @param records     the splitter's {@code records}
     * @param inbox       the splitter's {@code inbox}
     * @param landedItems the landing node's {@code landed_items}, read only
     */
    public record Tables(Table records, Table inbox,
            LandedItemsTable landedItems)
    {
    }

    /**
     * Holds what has been started.
     *
     * @param camel          stops the route on close
     * @param dataSource     the lease database's pool
     * @param catalog        the splitter's catalog
     * @param landingCatalog the landing catalog
     * @param lease          the splitter's lease
     * @param publisher      sends messages
     * @param pipeline       splits, stores and publishes
     * @param processor      takes items through the pipeline
     * @param inbox          where items wait
     * @param receiver       takes messages into the inbox
     * @param tables         the tables
     * @param keys           the keys
     * @param ids            makes IDs and MACs
     * @param committer      makes fenced commits
     * @param retry          how transient failures are retried
     * @param results        sends final results; null if they are off
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private ConnectorRuntime(CamelContext camel, HikariDataSource dataSource,
            Catalog catalog, Catalog landingCatalog, LeaseKeeper lease,
            ClosablePublisher publisher, IngestPipeline pipeline,
            ItemProcessor processor, Inbox inbox, InboxReceiver receiver,
            Tables tables, KeyMaterial keys, RecordIds ids,
            FencedCommitter committer, RetryPolicy retry,
            ResultsChannel results)
    {
        this.camel = camel;
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.landingCatalog = landingCatalog;
        this.lease = lease;
        this.publisher = publisher;
        this.pipeline = pipeline;
        this.processor = processor;
        this.inbox = inbox;
        this.receiver = receiver;
        this.tables = tables;
        this.keys = keys;
        this.ids = ids;
        this.committer = committer;
        this.retry = retry;
        this.results = results;
    }

    /**
     * Starts the splitter's runtime.
     *
     * @param properties the splitter's settings
     * @param camel      the Camel context, for the publisher
     * @param fatal      stops the process, on lease loss and the like
     * @return the started runtime
     * @throws Exception if a setting is wrong, a key can't be loaded, the
     *                   lease is held, or storage can't be reached
     */
    public static ConnectorRuntime start(ConnectorProperties properties,
            CamelContext camel, FatalExit fatal) throws Exception
    {
        ConnectorProperties.Landing landing = properties.landing();
        String intake = require(landing.intake(), "sdh.landing.intake");
        require(landing.queueUri(), "sdh.landing.queue-uri");
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
        QueueType queueType = QueueType.of(properties.queue().type());
        String queueUri = queueType.effectiveUri(properties.queue().uri());
        KeyMaterial keys = KeyMaterial.load(properties.crypto());

        HikariDataSource dataSource = null;
        LeaseKeeper lease = null;
        Catalog catalog = null;
        Catalog landingCatalog = null;
        ClosablePublisher publisher = null;
        IngestPipeline pipeline = null;
        ResultsChannel results = null;
        try
        {
            dataSource = LeaseDatabase.dataSource(properties.lease().jdbc());
            lease = acquire(properties, dataSource, fatal);

            String namespace = properties.storage().namespace();
            catalog = CatalogUtil.buildIcebergCatalog(
                    properties.storage().catalogName(), catalogProperties,
                    null);
            Table records = IngestTables.records(catalog, namespace);
            Table inboxTable = InboxTables.inbox(catalog, namespace);
            FencedCommitter committer = new FencedCommitter(lease);
            committer.establish(records);
            committer.establish(inboxTable);
            String recordsKek = properties.crypto().keys().recordsKek();
            RecordIds ids = new RecordIds(IdKeys.loadOrCreate(records,
                    committer, keys.wrap(), recordsKek));

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
                    properties.queue().artemis().brokerUrl(),
                    properties.queue().artemis().username(),
                    properties.queue().artemis().password());
            publisher = Publishers.create(camel, queueType, queueUri);
            pipeline = new IngestPipeline(
                    pipelineSettings(properties, format, split, retry),
                    records, committer, keys.wrap(), ids, splitters,
                    new MessageSealer(keys.signer(), keys.signerKid(),
                            keys.recipient(), keys.recipientKid()),
                    publisher, clock);
            if (properties.results().enabled())
            {
                results = ResultsChannel.open(properties.results(),
                        properties.queue(), camel, keys.signer(),
                        keys.signerKid(), retry, ACK_TIMEOUT);
            }
            ItemProcessor processor = new ItemProcessor(pipeline);
            Inbox inbox = new Inbox(inboxTable, committer, ids,
                    properties.connectorId(), retry, clock);
            inbox.load();
            InboxReceiver receiver = new InboxReceiver(inbox, keys.inbox(),
                    intake, landing.storage().catalogName(),
                    landing.storage().namespace());
            Telemetry.get().audit("connector.started",
                    Fields.of(Attr.FORMAT, format).with(Attr.INTAKE, intake)
                            .with(Attr.EPOCH, lease.epoch()));
            return new ConnectorRuntime(camel, dataSource, catalog,
                    landingCatalog, lease, publisher, pipeline, processor,
                    inbox, receiver,
                    new Tables(records, inboxTable, landedItems), keys, ids,
                    committer, retry, results);
        }
        catch (Exception | Error failure)
        {
            if (pipeline != null)
            {
                pipeline.close();
            }
            closeQuietly(results);
            closeQuietly(publisher);
            if (landingCatalog != catalog)
            {
                closeQuietly(landingCatalog);
            }
            closeQuietly(catalog);
            closeQuietly(lease);
            closeQuietly(dataSource);
            keys.close();
            throw failure;
        }
    }

    /**
     * Takes the splitter's lease, waiting one TTL for a crashed holder's
     * lease to run out, then failing with {@code LEASE_HELD}.
     *
     * @param properties the splitter's settings
     * @param dataSource the lease database
     * @param fatal      stops the process if the lease is lost
     * @return the lease
     * @throws Exception if it can't be taken
     */
    private static LeaseKeeper acquire(ConnectorProperties properties,
            HikariDataSource dataSource, FatalExit fatal) throws Exception
    {
        Lease settings = properties.lease();
        JdbcLeaseStore store = new JdbcLeaseStore(dataSource,
                settings.jdbc().table(), Duration.ofSeconds(10));
        store.createTableIfMissing();
        return LeaseKeeper.acquire(store, settings.name(),
                LeaseKeeper.defaultOwner(),
                new LeaseKeeper.Settings(settings.ttl(),
                        settings.renewInterval(), settings.safetyMargin()),
                lost -> fatal.exit("LEASE_LOST:" + lost.reason()));
    }

    /**
     * The pipeline's settings, from the splitter's.
     *
     * @param properties the splitter's settings
     * @param format     the format
     * @param split      the checked split settings
     * @param retry      how transient failures are retried
     * @return the pipeline's settings
     */
    private static PipelineSettings pipelineSettings(
            ConnectorProperties properties, String format, SplitConfig split,
            RetryPolicy retry)
    {
        ConnectorProperties.Split splitSettings = properties.split();
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
                .withRetry(retry)
                .withConversion(splitSettings.convert()
                                ? ConverterRegistry.load()
                                : ConverterRegistry.none(),
                        splitSettings.convertMaxBytes().toBytes())
                .withConvertMemory(splitSettings.convertMemory() == null ? 0
                        : splitSettings.convertMemory().toBytes());
    }

    /**
     * The split settings, with the format's defaults where none are set.
     *
     * @param settings the {@code sdh.split} settings
     * @param format   the format
     * @return the split settings
     */
    static SplitConfig splitConfig(ConnectorProperties.Split settings,
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
     * Takes items through the pipeline.
     *
     * @return the processor
     */
    public ItemProcessor processor()
    {
        return processor;
    }

    /**
     * Where items wait.
     *
     * @return the inbox
     */
    public Inbox inbox()
    {
        return inbox;
    }

    /**
     * Takes {@code item.landed} messages into the inbox, for the route.
     *
     * @return the receiver
     */
    public InboxReceiver receiver()
    {
        return receiver;
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
     * Makes fenced commits under the lease, for maintenance.
     *
     * @return the committer
     */
    public FencedCommitter committer()
    {
        return committer;
    }

    /**
     * Makes IDs and MACs with the splitter's ID key.
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
     * How transient failures are retried.
     *
     * @return the policy
     */
    public RetryPolicy retry()
    {
        return retry;
    }

    /**
     * Sends final results to the intake's producer.
     *
     * @return the channel; empty if results are off
     */
    public Optional<ResultsChannel> results()
    {
        return Optional.ofNullable(results);
    }

    /** Stops the route, then closes everything in reverse order. */
    @Override
    public void close()
    {
        try
        {
            camel.getRouteController().stopRoute(InboxRoute.ROUTE_ID);
        }
        catch (Exception failure)
        {
            LOG.warn("connector.stop_route_failed error={}",
                    failure.getClass().getSimpleName());
        }
        pipeline.close();
        closeQuietly(results);
        closeQuietly(publisher);
        keys.close();
        closeQuietly(lease);
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
