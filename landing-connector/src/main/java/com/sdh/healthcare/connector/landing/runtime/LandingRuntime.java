/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.runtime;

import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandingLedger;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.LedgerIndex;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.landing.config.BrokerSettings;
import com.sdh.healthcare.connector.landing.config.HttpSettings;
import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.landing.intake.Announcer;
import com.sdh.healthcare.connector.landing.intake.AzureBlobSource;
import com.sdh.healthcare.connector.landing.intake.BatchService;
import com.sdh.healthcare.connector.landing.intake.GcsSource;
import com.sdh.healthcare.connector.landing.intake.IcebergTableSource;
import com.sdh.healthcare.connector.landing.intake.IntakeNotWritableException;
import com.sdh.healthcare.connector.landing.intake.IntakeService;
import com.sdh.healthcare.connector.landing.intake.LandingIntake;
import com.sdh.healthcare.connector.landing.intake.PushService;
import com.sdh.healthcare.connector.landing.intake.IntakeSource;
import com.sdh.healthcare.connector.landing.intake.LocalDirectorySource;
import com.sdh.healthcare.connector.landing.intake.Readiness;
import com.sdh.healthcare.connector.landing.intake.S3Source;
import com.sdh.healthcare.connector.landing.intake.SftpSource;
import com.sdh.healthcare.connector.landing.intake.SmbSource;
import com.sdh.healthcare.connector.landing.intake.TableProgress;
import com.sdh.healthcare.connector.landing.route.EventHubsSupport;
import com.sdh.healthcare.connector.landing.route.IntakeRoute;
import com.sdh.healthcare.connector.landing.web.PushController;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import com.sdh.healthcare.connector.runtime.LeaseDatabase;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.publish.ClosablePublisher;
import com.sdh.healthcare.connector.runtime.publish.Publishers;
import com.sdh.healthcare.connector.runtime.publish.QueueType;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

/**
 * Everything a landing node runs on, started in a fixed order (design
 * §5.3.5): settings and keys → lease (waiting as a standby if set) → the
 * directory claim and write probe of a read-write source → catalog and
 * tables, fenced with the lease's epoch → ID key → lander, ledger and
 * its index → publisher. Closed in reverse, after the route has stopped,
 * so no item is in flight when the lease is released.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LandingRuntime implements AutoCloseable
{

    /** How long to wait for the broker to acknowledge a message. */
    static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);

    /** Logs start and stop. */
    private static final Logger LOG =
            LoggerFactory.getLogger(LandingRuntime.class);

    /** Stops the route on close. */
    private final CamelContext camel;

    /** The lease database's pool. */
    private final HikariDataSource dataSource;

    /** The landing catalog. */
    private final Catalog catalog;

    /** The intake's lease. */
    private final LeaseKeeper lease;

    /** Sends messages. */
    private final ClosablePublisher publisher;

    /** Sends receipts to the producer; null when results are off. */
    private final ResultsChannel results;

    /** The keys, closed with the runtime. */
    private final KeyMaterial keys;

    /** The source's checked settings. */
    private final SourceSettings source;

    /** Where items come from. */
    private final IntakeSource intakeSource;

    /** Lands a listing intake's items; null for an HTTP intake. */
    private final IntakeService service;

    /** Lands an HTTP intake's pushes; null for a listing intake. */
    private final PushService pushService;

    /** Lands a queue intake's batches; null for other intakes. */
    private final BatchService batchService;

    /** A queue intake's broker settings; null for other intakes. */
    private final BrokerSettings broker;

    /** A RabbitMQ intake's connection factory; null for other intakes. */
    private final CachingConnectionFactory rabbitmq;

    /** The landing tables, for maintenance. */
    private final Tables tables;

    /**
     * The landing tables and what writes them, for table maintenance.
     *
     * @param committer   fences commits with the lease
     * @param landedItems the {@value LandingTables#LANDED_ITEMS} table
     * @param ledger      the {@value LandingTables#LEDGER} table
     * @param lander      lands items; holds the intake's checkpoint
     */
    public record Tables(FencedCommitter committer, Table landedItems,
            Table ledger, ItemLander lander)
    {
    }

    /**
     * Holds what has been started.
     *
     * @param camel      stops the route on close
     * @param dataSource the lease database's pool
     * @param catalog    the landing catalog
     * @param lease      the intake's lease
     * @param publisher  sends messages
     * @param results    sends receipts; null when results are off
     * @param keys       the keys
     * @param source     the source's settings
     * @param intakeSource where items come from
     * @param service    lands a listing intake's items; null for HTTP
     * @param pushService lands an HTTP intake's pushes; null otherwise
     * @param queue      a queue intake's service, broker and connection;
     *                   null otherwise
     * @param tables     the landing tables
     */
    private LandingRuntime(CamelContext camel, HikariDataSource dataSource,
            Catalog catalog, LeaseKeeper lease, ClosablePublisher publisher,
            ResultsChannel results, KeyMaterial keys, SourceSettings source,
            IntakeSource intakeSource, IntakeService service,
            PushService pushService, QueueIntake queue, Tables tables)
    {
        this.camel = camel;
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.lease = lease;
        this.publisher = publisher;
        this.results = results;
        this.keys = keys;
        this.source = source;
        this.intakeSource = intakeSource;
        this.service = service;
        this.pushService = pushService;
        this.batchService = queue == null ? null : queue.service();
        this.broker = queue == null ? null : queue.broker();
        this.rabbitmq = queue == null ? null : queue.rabbitmq();
        this.tables = tables;
    }

    /**
     * A queue intake's parts.
     *
     * @param service  lands its batches
     * @param broker   its broker's settings
     * @param rabbitmq its RabbitMQ connection factory; null for other
     *                 brokers
     */
    private record QueueIntake(BatchService service, BrokerSettings broker,
            CachingConnectionFactory rabbitmq)
    {
    }

    /**
     * Starts the node's runtime.
     *
     * @param properties the node's settings
     * @param camel      the Camel context, for the publisher
     * @param fatal      stops the process, on lease loss and the like
     * @return the started runtime
     * @throws Exception if a setting is wrong, a key can't be loaded, the
     *                   lease is held, or storage can't be reached
     */
    public static LandingRuntime start(LandingProperties properties,
            CamelContext camel, FatalExit fatal) throws Exception
    {
        String intake = require(properties.landing().intake(),
                "sdh.landing.intake");
        SourceSettings source =
                SourceSettings.of(properties.landing().source());
        HttpSettings http = null;
        if (source.type() == SourceSettings.Type.HTTP)
        {
            PushController.path(intake);
            http = HttpSettings.of(properties.landing().http());
        }
        if (properties.results().enabled() && http != null
                && !http.oneSender())
        {
            throw new IllegalArgumentException("sdh.results needs an HTTP"
                    + " intake with one sender: set"
                    + " sdh.landing.http.oauth2.subject, or list one"
                    + " sdh.landing.http.mtls.subjects");
        }
        BrokerSettings broker = null;
        if (source.type() == SourceSettings.Type.QUEUE)
        {
            broker = BrokerSettings.of(properties.landing().broker());
        }
        LandingProperties.Storage storageSettings = properties.storage();
        Map<String, String> catalogProperties = require(
                storageSettings.catalog(), "sdh.storage.catalog");
        QueueType queueType = QueueType.of(properties.queue().type());
        String queueUri = queueType.effectiveUri(properties.queue().uri());
        KeyMaterial keys = KeyMaterial.forLanding(properties.crypto(),
                KeyMaterial.Providers.DEFAULT);

        HikariDataSource dataSource = null;
        LeaseKeeper lease = null;
        Catalog catalog = null;
        ClosablePublisher publisher = null;
        ResultsChannel results = null;
        IntakeSource intakeSource = null;
        CachingConnectionFactory rabbitmq = null;
        try
        {
            dataSource = LeaseDatabase.dataSource(properties.lease().jdbc());
            lease = acquire(properties, dataSource, fatal);
            intakeSource = switch (source.type())
            {
                case DIRECTORY -> new LocalDirectorySource(source);
                case SFTP -> new SftpSource(source);
                case SMB -> new SmbSource(source);
                case S3 -> new S3Source(source);
                case AZURE_BLOB -> new AzureBlobSource(source);
                case GCS -> new GcsSource(source);
                case ICEBERG -> new IcebergTableSource(
                        properties.landing().intake(), source);
                case HTTP, QUEUE -> null;
            };
            InboundDirectoryClaim claim = null;
            if (source.writable() && !source.remote())
            {
                claim = InboundDirectoryClaim.claim(source.path(),
                        new InboundDirectoryClaim.Owner(
                                properties.connectorId(),
                                properties.leaseName(),
                                storageSettings.catalogName(),
                                storageSettings.namespace()));
            }
            if (source.writable())
            {
                checkWritable(intakeSource, intake);
            }

            catalog = CatalogUtil.buildIcebergCatalog(
                    storageSettings.catalogName(), catalogProperties, null);
            Table items = LandingTables.landedItems(catalog,
                    storageSettings.namespace());
            Table ledgerTable = LandingTables.ledger(catalog,
                    storageSettings.namespace());
            FencedCommitter committer = new FencedCommitter(lease);
            committer.establish(items);
            committer.establish(ledgerTable);
            String kek = properties.crypto().keys().landedItemsKek();
            SectionIds ids = new SectionIds(IdKeys.loadOrCreate(items,
                    committer, keys.wrap(), kek));

            RetryPolicy retry = new RetryPolicy(
                    properties.retry().maxAttempts(),
                    properties.retry().initialBackoff(),
                    properties.retry().maxBackoff());
            Clock clock = Clock.systemUTC();
            ItemLander.Settings landing = new ItemLander.Settings(intake,
                    kek, storageSettings.commitBytes().toBytes(), retry,
                    SectionCrypto.CHUNK_SIZE, ItemLander.DEFAULT_BLOCK);
            ItemLander lander = new ItemLander(items, committer, keys.wrap(),
                    ids, landing, clock);
            LandingLedger ledger =
                    new LandingLedger(ledgerTable, committer, retry);
            LedgerIndex index = ledger.load(intake);
            if (intakeSource instanceof IcebergTableSource tableSource)
            {
                tableSource.attach(new TableProgress(ledgerTable, committer,
                        ids, intake));
            }

            Publishers.configureBroker(camel, queueType, properties.queue());
            publisher = Publishers.create(camel, queueType, queueUri);
            Announcer.Receipts receipts = null;
            if (properties.results().enabled())
            {
                results = ResultsChannel.open(properties.results(),
                        properties.queue(), camel, keys.signer(),
                        keys.signerKid(), retry, ACK_TIMEOUT);
                receipts = new Announcer.Receipts(results, items,
                        keys.wrap());
            }
            Announcer announcer = new Announcer(
                    new MessageSealer(keys.signer(), keys.signerKid(),
                            keys.recipient(), keys.recipientKid()),
                    publisher, storageSettings.catalogName(),
                    storageSettings.namespace(), retry, ACK_TIMEOUT,
                    receipts);
            IntakeService.Storage storage =
                    new IntakeService.Storage(lander, ledger, index, ids);
            IntakeService service = null;
            PushService pushService = null;
            QueueIntake queue = null;
            if (http != null)
            {
                pushService = new PushService(intake, storage, announcer,
                        fatal, clock, http.maxItemBytes(), http.maxWait());
            }
            else if (broker != null)
            {
                if (broker.kind() == BrokerSettings.Kind.RABBITMQ)
                {
                    rabbitmq = rabbitmq(broker.rabbitmq());
                    camel.getRegistry().bind(BrokerSettings.RABBITMQ_BEAN,
                            rabbitmq);
                }
                if (broker.kind() == BrokerSettings.Kind.EVENTHUBS)
                {
                    EventHubsSupport.bind(camel, broker.eventhubs());
                }
                queue = new QueueIntake(new BatchService(intake, storage,
                        items, announcer, fatal, clock, broker.dedupWindow()),
                        broker, rabbitmq);
            }
            else
            {
                service = new IntakeService(intake, intakeSource,
                        new Readiness(source.settle(), System::nanoTime),
                        storage, announcer, claim, fatal, clock,
                        properties.retry().initialBackoff());
            }
            Telemetry.get().audit("connector.started",
                    Fields.of(Attr.INTAKE, intake)
                            .with(Attr.EPOCH, lease.epoch()));
            return new LandingRuntime(camel, dataSource, catalog, lease,
                    publisher, results, keys, source, intakeSource, service,
                    pushService, queue,
                    new Tables(committer, items, ledgerTable, lander));
        }
        catch (Exception | Error failure)
        {
            closeQuietly(intakeSource);
            if (rabbitmq != null)
            {
                rabbitmq.destroy();
            }
            closeQuietly(publisher);
            closeQuietly(results);
            closeQuietly(catalog);
            closeQuietly(lease);
            closeQuietly(dataSource);
            keys.close();
            throw failure;
        }
    }

    /**
     * Takes the intake's lease: as a standby, waiting for as long as it
     * takes; otherwise waiting one TTL for a crashed holder's lease to run
     * out, then failing with {@code LEASE_HELD}.
     *
     * @param properties the node's settings
     * @param dataSource the lease database
     * @param fatal      stops the process if the lease is lost
     * @return the lease
     * @throws Exception if it can't be taken
     */
    private static LeaseKeeper acquire(LandingProperties properties,
            HikariDataSource dataSource, FatalExit fatal) throws Exception
    {
        var settings = properties.lease();
        JdbcLeaseStore store = new JdbcLeaseStore(dataSource,
                settings.jdbc().table(), Duration.ofSeconds(10));
        store.createTableIfMissing();
        LeaseKeeper.Settings timing = new LeaseKeeper.Settings(
                settings.ttl(), settings.renewInterval(),
                settings.safetyMargin());
        String owner = LeaseKeeper.defaultOwner();
        if (properties.landing().standby())
        {
            return LeaseKeeper.acquireAsStandby(store, properties.leaseName(),
                    owner, timing,
                    lost -> fatal.exit("LEASE_LOST:" + lost.reason()));
        }
        return LeaseKeeper.acquire(store, properties.leaseName(), owner,
                timing, lost -> fatal.exit("LEASE_LOST:" + lost.reason()));
    }

    /**
     * Runs the write probe of a read-write source, reporting a failure.
     * A remote source that can't be reached stops startup too: the node
     * never lands items it couldn't then clear.
     *
     * @param intakeSource the source
     * @param intake       the intake's name
     * @throws IntakeNotWritableException if it can't be written
     * @throws IOException                if a remote source can't be
     *                                    reached
     */
    private static void checkWritable(IntakeSource intakeSource,
            String intake) throws IOException
    {
        try
        {
            intakeSource.checkWritable();
        }
        catch (IntakeNotWritableException failure)
        {
            Telemetry.get().auditError("intake.not_writable",
                    Fields.of(Attr.INTAKE, intake)
                            .with(Attr.ERROR_CODE, failure.code()));
            throw failure;
        }
    }

    /**
     * A listing intake's service, for the route.
     *
     * @return the service; null for an HTTP intake
     */
    public IntakeService service()
    {
        return service;
    }

    /**
     * An HTTP intake's service, for the route.
     *
     * @return the service; null for a listing intake
     */
    public PushService pushService()
    {
        return pushService;
    }

    /**
     * A queue intake's service, for the route.
     *
     * @return the service; null for other intakes
     */
    public BatchService batchService()
    {
        return batchService;
    }

    /**
     * A queue intake's broker settings, for the route.
     *
     * @return the settings; null for other intakes
     */
    public BrokerSettings broker()
    {
        return broker;
    }

    /**
     * Makes a queue intake's RabbitMQ connection factory.
     *
     * @param settings the connection settings
     * @return the factory
     * @throws Exception if TLS can't be set up
     */
    private static CachingConnectionFactory rabbitmq(
            LandingProperties.Rabbitmq settings) throws Exception
    {
        com.rabbitmq.client.ConnectionFactory client =
                new com.rabbitmq.client.ConnectionFactory();
        if (settings.ssl())
        {
            client.useSslProtocol(javax.net.ssl.SSLContext.getDefault());
            client.enableHostnameVerification();
        }
        CachingConnectionFactory factory =
                new CachingConnectionFactory(client);
        factory.setAddresses(settings.addresses());
        factory.setVirtualHost(settings.virtualHost());
        if (settings.username() != null)
        {
            factory.setUsername(settings.username());
        }
        if (settings.password() != null)
        {
            factory.setPassword(settings.password());
        }
        return factory;
    }

    /**
     * The intake, whichever kind it is: for recovery and maintenance.
     *
     * @return the intake
     */
    public LandingIntake intake()
    {
        return service != null ? service
                : pushService != null ? pushService : batchService;
    }

    /**
     * The landing tables, for maintenance.
     *
     * @return the tables
     */
    public Tables tables()
    {
        return tables;
    }

    /**
     * Where items come from, for the route.
     *
     * @return the source
     */
    public IntakeSource intakeSource()
    {
        return intakeSource;
    }

    /**
     * The source's checked settings, for the route.
     *
     * @return the settings
     */
    public SourceSettings source()
    {
        return source;
    }

    /** Stops the route, then closes everything in reverse order. */
    @Override
    public void close()
    {
        if (service != null || batchService != null)
        {
            try
            {
                camel.getRouteController().stopRoute(IntakeRoute.ROUTE_ID);
            }
            catch (Exception failure)
            {
                LOG.warn("connector.stop_route_failed error={}",
                        failure.getClass().getSimpleName());
            }
        }
        closeQuietly(intakeSource);
        if (rabbitmq != null)
        {
            rabbitmq.destroy();
        }
        closeQuietly(publisher);
        closeQuietly(results);
        keys.close();
        closeQuietly(lease);
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
    private static <T> T require(T value, String setting)
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
