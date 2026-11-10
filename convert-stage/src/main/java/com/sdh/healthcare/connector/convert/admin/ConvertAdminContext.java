/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import com.sdh.healthcare.connector.broker.send.ClosablePublisher;
import com.sdh.healthcare.connector.broker.send.Publishers;
import com.sdh.healthcare.connector.broker.send.QueueType;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.convert.ConvertInputs;
import com.sdh.healthcare.connector.convert.ConvertProcessor;
import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.runtime.publish.ResultsChannel;
import com.sdh.healthcare.connector.stage.RegistryReport;
import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import com.sdh.healthcare.connector.stage.StageBuild;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.ReprocessAdmin;
import com.sdh.healthcare.connector.stage.admin.StageAdmin;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * What the convert stage's admin commands read from: the lease database
 * only, opened on first use, read-only (design §11.4). Only
 * {@code decline} and {@code reopen} write, to the fail store, over a
 * connection of their own.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ConvertAdminContext
        implements StageAdmin, ReprocessAdmin, AutoCloseable
{

    /** The stage's settings. */
    private final ConvertProperties properties;

    /** The lease database. */
    private final AdminDatabase database;

    /** The lease database, writing: for the fail store's changes. */
    private final AdminDatabase writing;

    /** The keys, once loaded. */
    private KeyMaterial keys;

    /** The stage's catalog, once built. */
    private Catalog catalog;

    /** The Camel context the publishers send through, once started. */
    private CamelContext camel;

    /** The publisher to the stage's output, once created. */
    private ClosablePublisher publisher;

    /** The producer's results channel, once opened. */
    private ResultsChannel results;

    /**
     * Creates the context; nothing is opened yet.
     *
     * @param properties the stage's settings
     */
    ConvertAdminContext(ConvertProperties properties)
    {
        this.properties = properties;
        this.database = new AdminDatabase(properties.lease());
        this.writing = new AdminDatabase(properties.lease(), true);
    }

    /**
     * Creates the context with publishers of the caller's, for tests.
     *
     * @param properties the stage's settings
     * @param publisher  sends to the stage's output
     * @param results    sends to the producer; null for none
     */
    ConvertAdminContext(ConvertProperties properties,
            ClosablePublisher publisher, ResultsChannel results)
    {
        this(properties);
        this.publisher = publisher;
        this.results = results;
    }

    /**
     * The stage's settings.
     *
     * @return the settings
     */
    ConvertProperties properties()
    {
        return properties;
    }

    /**
     * The stage's keys, loaded on first use, as the stage loads them.
     *
     * @return the keys
     * @throws Exception if they can't be loaded
     */
    KeyMaterial keys() throws Exception
    {
        if (keys == null)
        {
            keys = KeyMaterial.forStage(properties.crypto(),
                    KeyMaterial.Providers.DEFAULT);
        }
        return keys;
    }

    /**
     * The stage's {@code records}, which must exist: loaded, never
     * created.
     *
     * @return the table
     * @throws IllegalArgumentException if no catalog is configured
     * @throws IllegalStateException    if the table doesn't exist
     */
    Table records()
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
        try
        {
            return catalog.loadTable(TableIdentifier.of(
                    properties.storage().namespace(),
                    RecordsTable.DEFAULT_NAME));
        }
        catch (NoSuchTableException missing)
        {
            throw new IllegalStateException("Table "
                    + RecordsTable.DEFAULT_NAME
                    + " does not exist: the stage has never run here");
        }
    }

    /**
     * The stage's input, {@code split_records}, which must exist.
     *
     * @return the table
     * @throws IllegalStateException if the table doesn't exist
     */
    Table splitRecords()
    {
        records();
        String namespace = properties.storage().effectiveSplitNamespace();
        try
        {
            return catalog.loadTable(TableIdentifier.of(namespace,
                    IngestTables.SPLIT_RECORDS));
        }
        catch (NoSuchTableException missing)
        {
            throw new IllegalStateException("Table "
                    + IngestTables.SPLIT_RECORDS
                    + " does not exist: the split stage has never run here");
        }
    }

    @Override
    public ReprocessStore reprocessRuns()
    {
        return new ReprocessStore(database.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public ReprocessStore reprocessRunsToChange()
    {
        return new ReprocessStore(writing.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public ReprocessInputs reprocessInputs()
    {
        return new ConvertInputs(splitRecords(),
                properties.storage().catalogName(),
                properties.storage().effectiveSplitNamespace());
    }

    @Override
    public String version()
    {
        return StageBuild.identity(ConvertProcessor.class);
    }

    /**
     * The Camel context the publishers send through, started on first use.
     *
     * @return the context
     */
    private CamelContext camel()
    {
        if (camel == null)
        {
            camel = new DefaultCamelContext();
            camel.start();
        }
        return camel;
    }

    /**
     * The publisher to the stage's output ({@code sdh.stage.output}),
     * created on first use, as the stage creates its own.
     *
     * @return the publisher
     */
    ClosablePublisher publisher()
    {
        if (publisher == null)
        {
            CommonSettings.Queue output = properties.stage().output();
            if (output == null || output.uri() == null)
            {
                throw new IllegalArgumentException(
                        "sdh.stage.output is required");
            }
            QueueType type = QueueType.of(output.type());
            CamelContext context = camel();
            Publishers.configureBroker(context, type,
                    output.artemis().brokerUrl(), output.artemis().username(),
                    output.artemis().password());
            publisher = Publishers.create(context, type,
                    type.effectiveUri(output.uri()));
        }
        return publisher;
    }

    /**
     * The producer's results channel, opened on first use, as the stage
     * opens it.
     *
     * @return the channel
     * @throws IllegalArgumentException if results are off
     * @throws Exception                if the producer's key can't be read
     */
    ResultsChannel results() throws Exception
    {
        if (results == null)
        {
            if (!properties.results().enabled())
            {
                throw new IllegalArgumentException(
                        "Results are off (sdh.results.enabled)");
            }
            publisher();
            KeyMaterial loaded = keys();
            results = ResultsChannel.open(properties.results(),
                    properties.stage().output(), camel(), loaded.signer(),
                    loaded.signerKid(), retry(), Duration.ofSeconds(30));
        }
        return results;
    }

    /**
     * How a send is tried again: the stage's retry settings.
     *
     * @return the policy
     */
    RetryPolicy retry()
    {
        return new RetryPolicy(properties.retry().maxAttempts(),
                properties.retry().initialBackoff(),
                properties.retry().maxBackoff());
    }

    @Override
    public String stage()
    {
        return ConvertProcessor.STAGE;
    }

    @Override
    public String connectorId()
    {
        return properties.connectorId();
    }

    @Override
    public JdbcLeaseStore leases()
    {
        return database.leases();
    }

    @Override
    public RegistryReport registry()
    {
        return new RegistryReport(database.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE);
    }

    @Override
    public FailureStore failures()
    {
        return new FailureStore(database.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public FailureStore failuresToChange()
    {
        return new FailureStore(writing.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE,
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

    @Override
    public long maxCorrectionBytes()
    {
        return properties.convert().maxBytes().toBytes();
    }

    @Override
    public CorrectionStore correctionsToChange()
    {
        return new CorrectionStore(writing.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    @Override
    public CorrectionStore corrections()
    {
        return new CorrectionStore(database.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE,
                Duration.ofSeconds(10));
    }

    /**
     * The failed record's row of {@code records}, decrypted: the record as
     * split, which is what failed to convert.
     */
    @Override
    public byte[] failedContent(UUID itemId, long seq) throws Exception
    {
        Table table = records();
        table.refresh();
        // The newest generation's, if the record failed in more than one.
        CommittedRecords.Row row = CommittedRecords.all(table, itemId)
                .stream()
                .filter(each -> each.seq() == seq && each.errorCode() != null)
                .reduce((older, newer) -> newer)
                .orElseThrow(() -> new IllegalStateException("The failed"
                        + " record isn't in " + RecordsTable.DEFAULT_NAME));
        long snapshot = table.currentSnapshot().snapshotId();
        ConnectorMessage.RecordInStorage message =
                (ConnectorMessage.RecordInStorage) ConvertProcessor
                        .recordMessage(itemId, row, new StorageRef(
                                properties.storage().catalogName(),
                                properties.storage().namespace(),
                                RecordsTable.DEFAULT_NAME, row.ingestedDate(),
                                snapshot, row.kekRef()));
        try (QueueReader reader = QueueReader.builder().catalog(catalog)
                .keyProvider(keys().wrap()).build())
        {
            return reader.readRecord(message,
                    (int) Math.min(Integer.MAX_VALUE - 8,
                            maxCorrectionBytes()));
        }
    }

    /**
     * Leaves the item to the stage's workers, which look again at items
     * whose failures changed (STAGES.md §15.3).
     */
    @Override
    public void changed(UUID itemId, AuditRecord audit, PrintStream out)
    {
        out.println("The stage's workers count the change within a renewal"
                + " interval: item.converted once none of the item's"
                + " failures is open, else item.progress");
    }

    @Override
    public void close()
    {
        closeQuietly(results);
        closeQuietly(publisher);
        closeQuietly(camel);
        if (keys != null)
        {
            keys.close();
        }
        closeQuietly(catalog);
        database.close();
        writing.close();
    }

    /**
     * Closes something, if it holds anything to close.
     *
     * @param resource the resource, or null
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
                // exiting
            }
        }
    }
}
