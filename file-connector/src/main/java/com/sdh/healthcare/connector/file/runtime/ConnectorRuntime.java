package com.sdh.healthcare.connector.file.runtime;

import com.sdh.healthcare.connector.contract.crypto.SectionCrypto;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.checkpoint.LeaseKeeper;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.FileProcessor;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.PipelineSettings;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitLimits;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.Archiver;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.config.KeyMaterial;
import com.sdh.healthcare.connector.file.publish.ClosablePublisher;
import com.sdh.healthcare.connector.file.publish.Publishers;
import com.sdh.healthcare.connector.file.publish.QueueType;
import com.sdh.healthcare.connector.file.source.DirectoryOutcomeRecorder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the connector runs on, started in a fixed order (design §9.3):
 * catalog and tables → lease → fence every table with our epoch → keys → ID key → pipeline, archivers, processor.
 * Closed in reverse; the inbound route is stopped first so no file is in flight when the lease is released.
 */
public final class ConnectorRuntime implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorRuntime.class);

    private final CamelContext camel;
    private final HikariDataSource dataSource;
    private final Catalog catalog;
    private final LeaseKeeper lease;
    private final ClosablePublisher publisher;
    private final IngestPipeline pipeline;
    private final FileProcessor processor;
    private final Table sections;
    private final Table sourceFiles;
    private final Table failedFiles;
    private final KeyMaterial keys;
    private final SectionIds ids;
    private final FencedCommitter committer;

    private ConnectorRuntime(CamelContext camel, HikariDataSource dataSource, Catalog catalog, LeaseKeeper lease,
            ClosablePublisher publisher, IngestPipeline pipeline, FileProcessor processor, Table sections,
            Table sourceFiles, Table failedFiles, KeyMaterial keys, SectionIds ids, FencedCommitter committer) {
        this.camel = camel;
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.lease = lease;
        this.publisher = publisher;
        this.pipeline = pipeline;
        this.processor = processor;
        this.sections = sections;
        this.sourceFiles = sourceFiles;
        this.failedFiles = failedFiles;
        this.keys = keys;
        this.ids = ids;
        this.committer = committer;
    }

    public static ConnectorRuntime start(ConnectorProperties p, CamelContext camel, FatalExit fatal) throws Exception {
        Path inbound = Required.require(p.file().inboundDir(), "sdh.file.inbound-dir");
        if (!Files.isDirectory(inbound)) {
            throw new IOException("Inbound directory does not exist");
        }
        String format = Required.require(p.split().format(), "sdh.split.format");
        SplitterRegistry splitters = SplitterRegistry.load();
        SplitConfig split = splitConfig(p.split(), format);
        splitters.get(format).validate(split);
        QueueType queueType = QueueType.of(p.queue().type());
        String queueUri = queueType.effectiveUri(p.queue().uri());
        KeyMaterial keys = KeyMaterial.load(p.crypto());

        HikariDataSource ds = null;
        Catalog catalog = null;
        LeaseKeeper lease = null;
        try {
            ds = dataSource(p.lease().jdbc());
            JdbcLeaseStore store = new JdbcLeaseStore(ds, p.lease().jdbc().table(), Duration.ofSeconds(10));
            store.createTableIfMissing();
            // Fails with LEASE_HELD (non-zero exit) if another instance is running.
            lease = LeaseKeeper.acquire(store, p.lease().name(), LeaseKeeper.defaultOwner(),
                    new LeaseKeeper.Settings(p.lease().ttl(), p.lease().renewInterval(), p.lease().safetyMargin()),
                    lost -> fatal.exit("LEASE_LOST:" + lost.reason()));

            Map<String, String> catalogProps = Required.require(p.storage().catalog(), "sdh.storage.catalog");
            catalog = CatalogUtil.buildIcebergCatalog(p.storage().catalogName(), catalogProps, null);
            Table sections = IngestTables.sections(catalog, p.storage().namespace());
            Table sourceFiles = IngestTables.sourceFiles(catalog, p.storage().namespace());
            Table failedFiles = IngestTables.failedFiles(catalog, p.storage().namespace());

            FencedCommitter committer = new FencedCommitter(lease);
            for (Table t : new Table[] {sections, sourceFiles, failedFiles}) {
                committer.establish(t);
            }
            String sectionsKek = p.crypto().keys().sectionsKek();
            SectionIds ids = new SectionIds(IdKeys.loadOrCreate(sections, committer, keys.wrap(), sectionsKek));

            RetryPolicy retry = new RetryPolicy(p.retry().maxAttempts(), p.retry().initialBackoff(),
                    p.retry().maxBackoff());
            int workers = p.encryptWorkers() > 0 ? p.encryptWorkers() : Runtime.getRuntime().availableProcessors();
            PipelineSettings settings = PipelineSettings.defaults(p.connectorId(), format, p.storage().catalogName(),
                            sectionsKek)
                    .withSplit(split)
                    .withCommit(p.storage().commit().maxSections(), p.storage().commit().maxBytes().toBytes(),
                            p.storage().commit().maxInterval())
                    .withWorkers(workers)
                    .withRetry(retry);
            configureBroker(camel, queueType, p.queue());
            ClosablePublisher publisher = Publishers.create(camel, queueType, queueUri);
            Clock clock = Clock.systemUTC();
            IngestPipeline pipeline = new IngestPipeline(settings, sections, committer, keys.wrap(), ids, splitters,
                    new MessageSealer(keys.signer(), keys.signerKid(), keys.recipient(), keys.recipientKid()),
                    publisher, clock);
            long archiveCommit = p.storage().archive().commitBytes().toBytes();
            Archiver sourceArchiver = new Archiver(sourceFiles, committer, keys.wrap(), ids, archiver(p,
                    p.crypto().keys().sourceFilesKek(), archiveCommit, retry), clock);
            Archiver failedArchiver = new Archiver(failedFiles, committer, keys.wrap(), ids, archiver(p,
                    p.crypto().keys().failedFilesKek(), archiveCommit, retry), clock);
            FileProcessor processor = new FileProcessor(pipeline, sourceArchiver, failedArchiver,
                    new DirectoryOutcomeRecorder(inbound), keys.recovery());
            LOG.info("connector.started connectorId={} format={} queue={} epoch={}", p.connectorId(), format,
                    queueType.name().toLowerCase(java.util.Locale.ROOT), lease.epoch());
            return new ConnectorRuntime(camel, ds, catalog, lease, publisher, pipeline, processor, sections,
                    sourceFiles, failedFiles, keys, ids, committer);
        } catch (Exception | Error e) {
            closeQuietly(lease);
            closeQuietly(catalog);
            closeQuietly(ds);
            throw e;
        }
    }

    private static Archiver.Settings archiver(ConnectorProperties p, String kek, long commitBytes, RetryPolicy retry) {
        return new Archiver.Settings(p.connectorId(), kek, commitBytes, retry, SectionCrypto.CHUNK_SIZE,
                Archiver.DEFAULT_BLOCK);
    }

    static SplitConfig splitConfig(ConnectorProperties.Split s, String format) {
        SplitConfig cfg = SplitConfig.defaults(format).withBoundary(s.boundary());
        if (s.options() != null) {
            for (Map.Entry<String, String> o : s.options().entrySet()) {
                cfg = cfg.withOption(o.getKey(), o.getValue());
            }
        }
        SplitLimits d = cfg.limits();
        ConnectorProperties.Limits l = s.limits();
        return cfg.withLimits(new SplitLimits(
                l.maxFileBytes() != null ? l.maxFileBytes().toBytes() : d.maxFileBytes(),
                l.maxSectionBytes() != null ? l.maxSectionBytes().toBytes() : d.maxSectionBytes(),
                l.maxSectionsPerFile() != null ? l.maxSectionsPerFile() : d.maxSectionsPerFile()));
    }

    private static HikariDataSource dataSource(ConnectorProperties.Jdbc jdbc) {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(Required.require(jdbc.url(), "sdh.lease.jdbc.url"));
        c.setUsername(jdbc.username());
        c.setPassword(jdbc.password());
        c.setMaximumPoolSize(2);
        c.setConnectionTimeout(10_000);
        c.setPoolName("sdh-lease");
        return new HikariDataSource(c);
    }

    /** Artemis needs a JMS connection factory on the jms component; other brokers configure themselves. */
    private static void configureBroker(CamelContext camel, QueueType type, ConnectorProperties.Queue q) {
        if (type == QueueType.ARTEMIS && q.artemis().brokerUrl() != null) {
            var cf = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(q.artemis().brokerUrl(),
                    q.artemis().username(), q.artemis().password());
            camel.getComponent("jms", org.apache.camel.component.jms.JmsComponent.class).setConnectionFactory(cf);
        }
    }

    public FileProcessor processor() {
        return processor;
    }

    public Catalog catalog() {
        return catalog;
    }

    public Table sections() {
        return sections;
    }

    public FencedCommitter committer() {
        return committer;
    }

    public Table sourceFiles() {
        return sourceFiles;
    }

    public Table failedFiles() {
        return failedFiles;
    }

    public SectionIds ids() {
        return ids;
    }

    public KeyMaterial keys() {
        return keys;
    }

    @Override
    public void close() {
        try {
            camel.getRouteController().stopRoute(InboundRouteIds.INBOUND);
        } catch (Exception e) {
            LOG.warn("connector.stop_route_failed error={}", e.getClass().getSimpleName());
        }
        pipeline.close();
        publisher.close();
        closeQuietly(lease);
        closeQuietly(catalog);
        closeQuietly(dataSource);
        LOG.info("connector.stopped");
    }

    private static void closeQuietly(Object o) {
        if (o instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
                // shutting down
            }
        }
    }

    /** Route ID shared with the route definition. */
    public static final class InboundRouteIds {
        public static final String INBOUND = "inbound";

        private InboundRouteIds() {}
    }

    /** Required-setting check with a clear message. */
    static final class Required {
        static <T> T require(T value, String name) {
            if (value == null || (value instanceof String s && s.isBlank())
                    || (value instanceof Map<?, ?> m && m.isEmpty())) {
                throw new IllegalArgumentException(name + " is required");
            }
            return value;
        }

        private Required() {}
    }
}
