/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.maintenance.MaintenanceSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Compaction;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Crypto;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Queue;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Results;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Retry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Telemetry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Orphans;
import com.sdh.healthcare.connector.runtime.config.ConnectorSettings;
import com.sdh.healthcare.connector.runtime.maintenance.MaintenanceScheduler;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the landing node (design §5.3, §14). One
 * node serves one intake. The settings both applications share are in
 * {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}; of
 * those, the node ignores {@code sdh.lease.name} and names its lease after
 * the intake (see {@link #leaseName()}), so two nodes for the same intake
 * always share one lease.
 *
 * @param connectorId the node's ID, recorded in audit events
 * @param landing     the intake and its source
 * @param storage     the landing catalog and namespace
 * @param lease       the lease database and timings
 * @param crypto      the keys and who holds them
 * @param queue       where {@code item.landed} messages go
 * @param retry       how transient failures are retried
 * @param telemetry   the telemetry export
 * @param maintenance table maintenance and retention
 * @param results     receipts for the intake's producer (design §5.3.8)
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh")
public record LandingProperties(
        @DefaultValue("landing-connector") String connectorId,
        @DefaultValue Landing landing,
        @DefaultValue Storage storage,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue Queue queue,
        @DefaultValue Retry retry,
        @DefaultValue Telemetry telemetry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Results results)
        implements ConnectorSettings
{

    /** What the lease's name starts with; the intake's name follows. */
    public static final String LEASE_PREFIX = "landing/";

    /**
     * The lease this node takes: {@code landing/<intake>}.
     *
     * @return the lease's name
     */
    public String leaseName()
    {
        return LEASE_PREFIX + landing.intake();
    }

    /**
     * The intake.
     *
     * @param intake  its name, unique within the landing namespace; it
     *                names the lease, the checkpoint and the ledger entries
     * @param standby wait for the lease for as long as it takes, as a
     *                standby for the active node, instead of exiting if
     *                it is held
     * @param source  where items are picked up
     * @param http    the push endpoint, for a source of type {@code http}
     * @param broker  the broker messages are consumed from, for a source of
     *                type {@code queue}
     */
    public record Landing(String intake,
            @DefaultValue("false") boolean standby,
            @DefaultValue Source source,
            @DefaultValue Http http,
            @DefaultValue Broker broker)
    {
    }

    /**
     * The broker a queue intake consumes from (design §5.3.16). Messages are
     * landed in batches, and acknowledged only once their batch is
     * committed. Secrets come from the environment, never the file.
     *
     * @param type        {@code kafka}, {@code sqs}, {@code rabbitmq},
     *                    {@code pubsub}, {@code servicebus} or
     *                    {@code eventhubs} (Camel consumers), or
     *                    {@code artemis} (the JMS client)
     * @param uri         the Camel consumer URI, for every type but
     *                    {@code artemis}; the options the intake needs are
     *                    added, and any that would break it refused
     * @param batch       when a batch is closed and landed
     * @param dedupWindow how far back, from the last landed batch, a message
     *                    delivered again is recognised by its ID
     * @param rabbitmq    the RabbitMQ connection, for {@code rabbitmq}
     * @param artemis     the Artemis connection and queue, for
     *                    {@code artemis}
     * @param eventhubs   the checkpoint store, for {@code eventhubs}
     */
    public record Broker(String type, String uri,
            @DefaultValue Batch batch,
            @DefaultValue("1h") Duration dedupWindow,
            @DefaultValue Rabbitmq rabbitmq,
            @DefaultValue Artemis artemis,
            @DefaultValue Eventhubs eventhubs)
    {
    }

    /**
     * Where an Event Hubs intake keeps its checkpoints: a blob container,
     * which must exist. Reached by a connection string, or by its endpoint
     * with the default Azure credential (managed or workload identity).
     *
     * @param checkpointConnectionString the storage account's connection
     *                                   string; from the environment
     * @param checkpointEndpoint         the blob endpoint, such as
     *                                   {@code https://a.blob.core.windows.net}
     * @param checkpointContainer        the container's name
     */
    public record Eventhubs(String checkpointConnectionString,
            String checkpointEndpoint, String checkpointContainer)
    {

        /**
         * The settings without the connection string.
         *
         * @return the settings as text
         */
        @Override
        public String toString()
        {
            return "Eventhubs[checkpointEndpoint=" + checkpointEndpoint
                    + ", checkpointContainer=" + checkpointContainer + "]";
        }
    }

    /**
     * When a batch of messages is closed: whichever limit comes first.
     *
     * @param maxMessages the most messages in a batch
     * @param maxBytes    the most bytes in a batch; a single larger message
     *                    is a batch of its own
     * @param maxWait     the longest a batch stays open after its first
     *                    message
     */
    public record Batch(@DefaultValue("1000") int maxMessages,
            @DefaultValue("64MB") DataSize maxBytes,
            @DefaultValue("5s") Duration maxWait)
    {
    }

    /**
     * A RabbitMQ connection.
     *
     * @param addresses   {@code host:port}, comma-separated
     * @param virtualHost the virtual host
     * @param username    the user
     * @param password    the password; from the environment
     * @param ssl         whether to connect over TLS
     */
    public record Rabbitmq(String addresses,
            @DefaultValue("/") String virtualHost, String username,
            String password, @DefaultValue("false") boolean ssl)
    {

        /**
         * The connection without its password.
         *
         * @return the settings as text
         */
        @Override
        public String toString()
        {
            return "Rabbitmq[addresses=" + addresses + ", virtualHost="
                    + virtualHost + ", username=" + username + ", ssl=" + ssl
                    + "]";
        }
    }

    /**
     * An Artemis connection and the queue to consume.
     *
     * @param brokerUrl the broker URL, such as {@code tcp://host:61616}
     * @param username  the user
     * @param password  the password; from the environment
     * @param queue     the queue's name
     */
    public record Artemis(String brokerUrl, String username, String password,
            String queue)
    {

        /**
         * The connection without its password.
         *
         * @return the settings as text
         */
        @Override
        public String toString()
        {
            return "Artemis[brokerUrl=" + brokerUrl + ", username=" + username
                    + ", queue=" + queue + "]";
        }
    }

    /**
     * The HTTP push endpoint (design §5.3.15): {@code POST
     * /intakes/<intake>/items}, over TLS, with OAuth2 or mTLS. Secrets come
     * from the environment, never the file.
     *
     * @param port         the HTTPS port
     * @param auth         {@code oauth2} (a bearer JWT) or {@code mtls} (a
     *                     client certificate)
     * @param tls          the server's certificate and key
     * @param oauth2       how bearer tokens are checked, for {@code oauth2}
     * @param mtls         which client certificates are accepted, for
     *                     {@code mtls}
     * @param maxItemBytes the largest body accepted; 0 for no limit
     * @param maxWait      how long a push waits while another is landed,
     *                     before it is turned away with 503
     * @param retryAfter   the {@code Retry-After} sent with a 503
     * @param readTimeout  how long the body may stall before the push is
     *                     dropped
     */
    public record Http(@DefaultValue("8443") int port,
            String auth,
            @DefaultValue Tls tls,
            @DefaultValue OAuth2 oauth2,
            @DefaultValue Mtls mtls,
            @DefaultValue("0") DataSize maxItemBytes,
            @DefaultValue("30s") Duration maxWait,
            @DefaultValue("30s") Duration retryAfter,
            @DefaultValue("60s") Duration readTimeout)
    {
    }

    /**
     * The server's TLS certificate, in PEM files.
     *
     * @param certificate        the certificate chain
     * @param privateKey         its private key
     * @param privateKeyPassword the key's password, if it is encrypted
     */
    public record Tls(Path certificate, Path privateKey,
            String privateKeyPassword)
    {
    }

    /**
     * How bearer tokens are checked: signed by the issuer's keys, for the
     * audience, with the scope.
     *
     * @param issuer    the issuer, as in the tokens' {@code iss} claim; its
     *                  keys are found through its OpenID discovery document
     * @param jwkSetUri the issuer's key set, when it has no discovery
     *                  document
     * @param audience  the audience the tokens must name
     * @param scope     the scope the tokens must carry
     * @param subject   the one sender accepted, as in the tokens'
     *                  {@code sub} claim; null to accept any subject.
     *                  Required with results on (design §5.3.8), so an
     *                  intake's results go to one producer only
     */
    public record OAuth2(String issuer, String jwkSetUri, String audience,
            String scope, String subject)
    {
    }

    /**
     * Which client certificates are accepted: issued by one of the trusted
     * CAs, with a subject on the list.
     *
     * @param trustedCas the CA certificates, in a PEM file
     * @param subjects   the accepted subjects, as distinguished names
     */
    public record Mtls(Path trustedCas, List<String> subjects)
    {
    }

    /**
     * A directory the node picks items up from.
     *
     * @param path         the directory
     * @param access       {@code read-only}, so nothing in it is changed,
     *                     or {@code read-write}, so each item is removed
     *                     from it after landing
     * @param after        what to do with an item after landing, on a
     *                     read-write source: {@code delete} (the default),
     *                     {@code move} or {@code rename}; {@code none} on a
     *                     read-only source
     * @param moveTo       where {@code move} puts items, keeping their
     *                     relative paths
     * @param renameSuffix what {@code rename} appends to an item's name;
     *                     names ending in it are never picked up
     * @param ready        when an item is ready: {@code settled}, once its
     *                     size and modification time have stayed the same
     *                     for {@code settle}, or {@code marker}, once a
     *                     {@code <name>.done} file sits beside it
     * @param settle       how long an item must stay unchanged, for
     *                     {@code settled}
     * @param include      an Ant-style pattern of the paths to pick up
     * @param recursive    whether to look in subdirectories
     * @param pollInterval how often the source is listed
     * @param type         {@code directory} (the default), {@code sftp},
     *                     {@code smb}, {@code s3}, {@code azure-blob},
     *                     {@code gcs}, {@code iceberg}, {@code http} or
     *                     {@code queue}
     * @param authRetryInterval how long to wait before trying a remote
     *                     source again after its credentials, permissions,
     *                     host key or share were refused, so an account
     *                     isn't locked by retries
     * @param sftp         the SFTP server, for {@code sftp}
     * @param smb          the SMB share, for {@code smb}
     * @param bucket       the bucket or container, for {@code s3},
     *                     {@code azure-blob} and {@code gcs}
     * @param table        the table, for {@code iceberg}
     */
    public record Source(Path path,
            @DefaultValue("read-only") String access,
            String after,
            Path moveTo,
            @DefaultValue(".landed") String renameSuffix,
            @DefaultValue("settled") String ready,
            @DefaultValue("2m") Duration settle,
            @DefaultValue("*") String include,
            @DefaultValue("false") boolean recursive,
            @DefaultValue("10s") Duration pollInterval,
            @DefaultValue("directory") String type,
            @DefaultValue("5m") Duration authRetryInterval,
            @DefaultValue Sftp sftp,
            @DefaultValue Smb smb,
            @DefaultValue Bucket bucket,
            @DefaultValue Table table)
    {
    }

    /**
     * An SFTP server (design §5.3.4). The source's {@code path} is the
     * directory on the server. The password and the key's passphrase come
     * from the environment, never the file.
     *
     * @param host                the server's name or address
     * @param port                its port
     * @param user                the account
     * @param privateKey          the account's private key file, if it uses
     *                            one
     * @param privateKeyPassphrase the key's passphrase, if it has one
     * @param password            the account's password, if it uses one
     * @param knownHosts          a {@code known_hosts} file holding the
     *                            server's host key; required, since an
     *                            unknown key is never accepted
     * @param connectTimeout      how long to wait for the connection
     * @param readTimeout         how long a connection may stall before
     *                            it is dropped
     */
    public record Sftp(String host,
            @DefaultValue("22") int port,
            String user,
            Path privateKey,
            String privateKeyPassphrase,
            String password,
            Path knownHosts,
            @DefaultValue("15s") Duration connectTimeout,
            @DefaultValue("60s") Duration readTimeout)
    {
    }

    /**
     * An SMB share (design §5.3.4), over SMB 3 with encryption required.
     * The source's {@code path} is the directory within the share,
     * {@code /} for its root. The password comes from the environment,
     * never the file.
     *
     * @param host           the server's name or address
     * @param port           its port
     * @param share          the share's name
     * @param domain         the account's domain; empty for a local account
     * @param user           the account
     * @param password       its password
     * @param connectTimeout how long to wait for the connection
     * @param readTimeout    how long a read or other request may wait
     */
    public record Smb(String host,
            @DefaultValue("445") int port,
            String share,
            @DefaultValue("") String domain,
            String user,
            String password,
            @DefaultValue("15s") Duration connectTimeout,
            @DefaultValue("60s") Duration readTimeout)
    {
    }

    /**
     * An object store bucket (design §5.3.17): an S3 bucket (or one on an
     * S3-compatible store), an Azure Blob container, or a GCS bucket. The
     * source's {@code path} and {@code move-to} are key prefixes, {@code /}
     * being the bucket's root. Secrets come from the environment, never the
     * file; without them, each cloud's default credentials are used.
     *
     * @param name             the bucket, or the Azure container
     * @param endpoint         the service's address: required for Azure
     *                         without a connection string (the account's
     *                         blob endpoint); for S3, an S3-compatible
     *                         store; for GCS, an emulator
     * @param region           the S3 region; default from the environment,
     *                         or {@code us-east-1} with an endpoint
     * @param pathStyleAccess  whether S3 requests put the bucket in the
     *                         path, as most S3-compatible stores need
     * @param accessKeyId      an S3 access key, with
     *                         {@code secret-access-key}
     * @param secretAccessKey  its secret
     * @param connectionString an Azure storage connection string
     * @param projectId        the GCS project; default from the environment
     * @param anonymous        whether to send no credentials, for an
     *                         emulator or a public bucket
     * @param connectTimeout   how long to wait for a connection
     * @param readTimeout      how long a request or read may stall before
     *                         it fails
     */
    public record Bucket(String name,
            String endpoint,
            String region,
            @DefaultValue("false") boolean pathStyleAccess,
            String accessKeyId,
            String secretAccessKey,
            String connectionString,
            String projectId,
            @DefaultValue("false") boolean anonymous,
            @DefaultValue("15s") Duration connectTimeout,
            @DefaultValue("60s") Duration readTimeout)
    {

        /**
         * Shows the settings without the secret or the connection string.
         *
         * @return the settings, secrets hidden
         */
        @Override
        public String toString()
        {
            return "Bucket[" + name + ", endpoint=" + endpoint + "]";
        }
    }

    /**
     * An Iceberg table an intake reads (design §5.3.18): each data file an
     * append adds is landed as one item. The catalog is the table's own,
     * not the landing catalog; its secrets come from the environment.
     *
     * @param catalog     Iceberg catalog properties, passed through as they
     *                    are: {@code type}, {@code uri}, {@code warehouse},
     *                    {@code io-impl}, and so on
     * @param catalogName the catalog's name
     * @param identifier  the table, as {@code namespace.table}
     * @param start       {@code earliest}, to land every row the table
     *                    holds and then each append, or {@code latest}, to
     *                    land only appends from the first start on
     */
    public record Table(Map<String, String> catalog,
            @DefaultValue("source") String catalogName,
            String identifier,
            @DefaultValue("earliest") String start)
    {

        /**
         * Shows the table without the catalog's properties, which may hold
         * a secret.
         *
         * @return the table's name
         */
        @Override
        public String toString()
        {
            return "Table[" + catalogName + ":" + identifier + "]";
        }
    }

    /**
     * The landing catalog (design §5.3.3). It may be a different catalog,
     * on different storage, from the one the splitter writes sections to.
     *
     * @param catalog     Iceberg catalog properties, passed through as they
     *                    are: {@code type}, {@code uri}, {@code warehouse},
     *                    {@code io-impl}, and so on
     * @param catalogName the catalog's name, also put in messages
     * @param namespace   the landing namespace
     * @param commitBytes commit a large item's chunks after at least this
     *                    many bytes, so a restart resumes from there
     */
    public record Storage(Map<String, String> catalog,
            @DefaultValue("sdh") String catalogName,
            @DefaultValue(LandingTables.DEFAULT_NAMESPACE) String namespace,
            @DefaultValue("512MB") DataSize commitBytes)
    {
    }

    /**
     * Table maintenance of the landing tables (design §5.3.13, §8.4), run
     * between items. The ledger is compacted and its snapshots expired, but
     * never purged: it is how a read-only source's files are recognised.
     *
     * @param enabled           whether it runs
     * @param interval          how often to try; a run is skipped while an
     *                          item is being landed
     * @param snapshotRetention how long old snapshots are kept
     * @param minSnapshots      the fewest snapshots kept
     * @param compaction        compaction of small files
     * @param orphans           removal of unreferenced files
     * @param retention         how long landed items are kept
     */
    public record Maintenance(@DefaultValue("true") boolean enabled,
            @DefaultValue("1h") Duration interval,
            @DefaultValue("7d") Duration snapshotRetention,
            @DefaultValue("10") int minSnapshots,
            @DefaultValue Compaction compaction,
            @DefaultValue Orphans orphans,
            @DefaultValue Retention retention)
    {

        /** The shortest retention allowed, so the splitter has time. */
        public static final Duration MIN_RETENTION = Duration.ofDays(7);

        /**
         * What each run does.
         *
         * @return the settings
         */
        public MaintenanceSettings toSettings()
        {
            return MaintenanceScheduler.settings(snapshotRetention,
                    minSnapshots, compaction, orphans);
        }

        /**
         * When it runs.
         *
         * @return the schedule
         */
        public MaintenanceScheduler.Schedule toSchedule()
        {
            return new MaintenanceScheduler.Schedule(enabled, interval,
                    orphans.enabled(), orphans.interval());
        }
    }

    /**
     * How long landed items are kept, in whole days; unset keeps them
     * forever (the default). At least {@link Maintenance#MIN_RETENTION}.
     *
     * @param landedItems the {@value LandingTables#LANDED_ITEMS} table
     */
    public record Retention(Duration landedItems)
    {
    }
}
