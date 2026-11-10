/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordLocation;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.storage.CommittedRecords;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.record.RecordTable;
import com.sdh.healthcare.connector.record.RecordTransform;
import com.sdh.healthcare.connector.record.SourceInputs;
import com.sdh.healthcare.connector.record.config.RecordStageSettings;
import com.sdh.healthcare.connector.record.runtime.StageTables;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.PartWay;
import com.sdh.healthcare.connector.stage.RegistryReport;
import com.sdh.healthcare.connector.stage.ReprocessInputs;
import com.sdh.healthcare.connector.stage.ReprocessStore;
import com.sdh.healthcare.connector.stage.StageBuild;
import com.sdh.healthcare.connector.stage.StageRegistry;
import com.sdh.healthcare.connector.stage.StageReleases;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.DrainAdmin;
import com.sdh.healthcare.connector.stage.admin.DrainCheckCommand;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.ReprocessCommand;
import com.sdh.healthcare.connector.stage.admin.StageAdmin;
import com.sdh.healthcare.connector.stage.admin.StageStatusCommand;
import com.sdh.healthcare.connector.stage.admin.VersionCommand;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * A record stage's operator commands (design §11.4, STAGES.md §17.3):
 * {@code status}, {@code reprocess}, {@code drain-check}, {@code version}
 * and those for its fail store, on the shared {@link AdminCli}. They read
 * the lease database only, opened on first use, read-only, and the source
 * and sink as {@code sdh.stage.source} and {@code sdh.stage.sink} name
 * them, never created. Only the fail store's commands write, to the fail
 * store, over a connection of their own. None sends to a broker (§15.8.10
 * step 5).
 *
 * @param <P> the stage's settings
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RecordStageAdmin<P extends RecordStageSettings>
        implements StageAdmin, DrainAdmin, AutoCloseable
{

    /** How long a database call may wait for a connection. */
    static final Duration DATABASE_WAIT = Duration.ofSeconds(10);

    /** The stage's settings. */
    private final P settings;

    /** The stage's messages. */
    private final StageMessages messages;

    /** Makes the stage's transform, for its versions and limits. */
    private final Function<P, RecordTransform> transforms;

    /** The lease database. */
    private final AdminDatabase database;

    /** The lease database, writing: for the fail store's changes. */
    private final AdminDatabase writing;

    /** The keys, once loaded. */
    private KeyMaterial keys;

    /** The source and sink, once their catalogs are built. */
    private StageTables tables;

    /** The transform, once made. */
    private RecordTransform transform;

    /**
     * Creates the context; nothing is opened yet.
     *
     * @param settings   the stage's settings
     * @param messages   the stage's messages
     * @param transforms makes the stage's transform from its settings, on
     *                   first use
     */
    RecordStageAdmin(P settings, StageMessages messages,
            Function<P, RecordTransform> transforms)
    {
        this.settings = settings;
        this.messages = messages;
        this.transforms = transforms;
        this.database = new AdminDatabase(settings.lease());
        this.writing = new AdminDatabase(settings.lease(), true);
    }

    /**
     * A record stage's CLI.
     *
     * @param <P>           the stage's settings
     * @param jar           the stage's jar, for the usage text
     * @param service       the stage's name as a service, for audit events
     * @param configuration binds {@code sdh.*} into the settings and
     *                      nothing else
     * @param type          the settings' type
     * @param messages      the stage's messages
     * @param transforms    makes the stage's transform from its settings
     * @return the CLI
     */
    public static <P extends RecordStageSettings> AdminCli<P, ?> cli(
            String jar, String service, Class<?> configuration,
            Class<P> type, StageMessages messages,
            Function<P, RecordTransform> transforms)
    {
        return new AdminCli<P, RecordStageAdmin<P>>(new AdminCli.Stage<>(
                jar, service, configuration, type,
                RecordStageSettings::telemetry,
                settings -> new RecordStageAdmin<>(settings, messages,
                        transforms)),
                commands());
    }

    /**
     * The commands, with those for the fail store.
     *
     * @param <P> the stage's settings
     * @return the commands
     */
    static <P extends RecordStageSettings>
            List<AdminCommand<RecordStageAdmin<P>>> commands()
    {
        List<AdminCommand<RecordStageAdmin<P>>> commands =
                new ArrayList<>(List.of(new StageStatusCommand<>(),
                        new ReprocessCommand<>(), new DrainCheckCommand<>(),
                        new VersionCommand<>()));
        commands.addAll(FailureCommands.<RecordStageAdmin<P>>all());
        return List.copyOf(commands);
    }

    /**
     * The stage's settings.
     *
     * @return the settings
     */
    public P settings()
    {
        return settings;
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
            keys = KeyMaterial.forStage(settings.crypto(),
                    KeyMaterial.Providers.DEFAULT);
        }
        return keys;
    }

    /**
     * The source and sink's catalogs, built on first use.
     *
     * @return them
     */
    private StageTables tables()
    {
        if (tables == null)
        {
            tables = new StageTables(settings, messages);
        }
        return tables;
    }

    /**
     * The transform, made on first use.
     *
     * @return it
     */
    private RecordTransform transform()
    {
        if (transform == null)
        {
            transform = transforms.apply(settings);
        }
        return transform;
    }

    @Override
    public ReprocessStore reprocessRuns()
    {
        return new ReprocessStore(database.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    @Override
    public ReprocessStore reprocessRunsToChange()
    {
        return new ReprocessStore(writing.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the source doesn't exist
     */
    @Override
    public ReprocessInputs reprocessInputs()
    {
        return new SourceInputs(tables().existingSource(), messages);
    }

    @Override
    public String version()
    {
        return StageBuild.identity(RecordStageAdmin.class);
    }

    /** Every version the stage's transform stamps. */
    @Override
    public Collection<String> releaseVersions()
    {
        return transform().versions();
    }

    @Override
    public StageReleases stageReleases()
    {
        return new StageReleases(database.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    @Override
    public StageReleases stageReleasesToChange()
    {
        return new StageReleases(writing.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    /** The registry's unfinished items with output, as the pods find. */
    @Override
    public PartWay partWay()
    {
        return new StageRegistry(database.dataSource(),
                settings.stage().registrySchema(), stage(), "admin",
                DATABASE_WAIT)::partWay;
    }

    @Override
    public String stage()
    {
        return messages.stage();
    }

    @Override
    public String connectorId()
    {
        return settings.connectorId();
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
                settings.stage().registrySchema(), stage());
    }

    @Override
    public FailureStore failures()
    {
        return new FailureStore(database.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    @Override
    public FailureStore failuresToChange()
    {
        return new FailureStore(writing.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    @Override
    public KeyProvider reasonKeys() throws Exception
    {
        return keys().wrap();
    }

    @Override
    public String reasonKek()
    {
        return StageTables.sinkKek(settings);
    }

    @Override
    public boolean corrects()
    {
        return true;
    }

    @Override
    public long maxCorrectionBytes()
    {
        return transform().maxBytes();
    }

    @Override
    public CorrectionStore correctionsToChange()
    {
        return new CorrectionStore(writing.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    @Override
    public CorrectionStore corrections()
    {
        return new CorrectionStore(database.dataSource(),
                settings.stage().registrySchema(), stage(), DATABASE_WAIT);
    }

    /**
     * The failed record's row of the sink, decrypted: the record as it
     * came, which is what failed here.
     *
     * @throws IllegalStateException if the sink doesn't exist, or the
     *                               record isn't in it
     */
    @Override
    public byte[] failedContent(UUID itemId, long seq) throws Exception
    {
        RecordTable sink = tables().existingSink();
        sink.table().refresh();
        // The newest generation's, if the record failed in more than one.
        CommittedRecords.Row row = CommittedRecords.all(sink.table(), itemId)
                .stream()
                .filter(each -> each.seq() == seq && each.errorCode() != null)
                .reduce((older, newer) -> newer)
                .orElseThrow(() -> new IllegalStateException("The failed"
                        + " record isn't in " + sink.name()));
        RecordLocation location = sink.location(itemId, row,
                sink.table().currentSnapshot().snapshotId());
        try (QueueReader reader = QueueReader.builder()
                .catalog(tables().sinkCatalog())
                .keyProvider(keys().wrap()).build())
        {
            return reader.readRecord(location,
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
                + " interval: the item's end once none of its failures is"
                + " open, else item.progress");
    }

    @Override
    public void close()
    {
        if (keys != null)
        {
            keys.close();
        }
        if (tables != null)
        {
            try
            {
                tables.close();
            }
            catch (Exception ignored)
            {
                // exiting
            }
        }
        if (transform instanceof AutoCloseable closeable)
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
        database.close();
        writing.close();
    }
}
