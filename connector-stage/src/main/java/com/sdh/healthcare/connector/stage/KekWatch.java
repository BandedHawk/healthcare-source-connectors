/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KEK a pod wraps each new batch's data key with
 * (CONTROL-API-DESIGN.md §11): its setting's, until its scope is rotated
 * ({@link ControlState#rotate}). It reads the scope's row when started and
 * every {@link #EVERY} after, as the pause is read.
 *
 * <p>A KEK rotated to is first probed, wrapping and unwrapping a throwaway
 * key: one the pod can't use is refused, audited once as
 * {@code keys.kek_refused}, and the KEK in use is kept, so nothing is
 * written that the pod couldn't read back. A read that fails keeps the
 * KEK in use. A batch keeps the KEK it started with; a rotation takes
 * effect at the next.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class KekWatch implements Supplier<String>, AutoCloseable
{

    /** How often the KEK is read. */
    public static final Duration EVERY = PauseWatch.EVERY;

    /** Logs changes and failures, by the failure's class alone. */
    private static final Logger LOG = LoggerFactory.getLogger(
            KekWatch.class);

    /** Reads the KEK the scope was rotated to. */
    @FunctionalInterface
    public interface Reader
    {

        /**
         * Reads it.
         *
         * @return the KEK; empty if the scope was never rotated
         * @throws RuntimeException if it can't be read
         */
        Optional<String> kek();
    }

    /** Checks the pod can wrap and unwrap with a KEK. */
    @FunctionalInterface
    public interface Probe
    {

        /**
         * Checks it.
         *
         * @param kek the KEK
         * @throws Exception if the pod can't use it
         */
        void probe(String kek) throws Exception;
    }

    /** Reads the rotated KEK. */
    private final Reader reader;

    /** The setting's KEK, used until rotated. */
    private final String setting;

    /** Checks a KEK rotated to. */
    private final Probe probe;

    /** The scope, for the log and audit. */
    private final String scope;

    /** How often it reads. */
    private final Duration every;

    /** The KEK in use. */
    private volatile String current;

    /** The last KEK refused, so it is audited once; null if none. */
    private String refused;

    /** Reads the KEK every interval; null until started. */
    private ScheduledExecutorService scheduler;

    /**
     * Creates a watch, not started. Until started, it gives the setting's
     * KEK.
     *
     * @param reader  reads the rotated KEK
     * @param setting the setting's KEK
     * @param probe   checks a KEK rotated to
     * @param scope   the scope, for the log and audit
     * @param every   how often it reads
     */
    public KekWatch(Reader reader, String setting, Probe probe, String scope,
            Duration every)
    {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.setting = Objects.requireNonNull(setting, "setting");
        this.probe = Objects.requireNonNull(probe, "probe");
        this.scope = scope;
        this.every = every;
        this.current = setting;
    }

    /**
     * Watches a scope's KEK every {@link #EVERY}, probing a KEK rotated to
     * with the pod's provider, and creating the table if it isn't there.
     *
     * @param state   the scope's state
     * @param setting the setting's KEK
     * @param keys    the pod's provider
     * @return the watch, not started
     * @throws IllegalStateException if the table can't be created
     */
    public static KekWatch of(ControlState state, String setting,
            KeyProvider keys)
    {
        state.createTableIfMissing();
        return new KekWatch(() -> state.kek().map(ControlState.Kek::kek),
                setting, kek -> KeyMaterial.probeKek(keys, kek),
                state.scope(), EVERY);
    }

    /** Reads the KEK, then reads it again every interval. */
    public synchronized void start()
    {
        if (scheduler != null)
        {
            return;
        }
        look();
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
        {
            Thread thread = new Thread(runnable, "sdh-kek-watch");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::look, every.toMillis(),
                every.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * The KEK to wrap a new batch's data key with.
     *
     * @return it
     */
    @Override
    public String get()
    {
        return current;
    }

    /** Reads the KEK, and takes it if it changed and the pod can use it. */
    synchronized void look()
    {
        String wanted;
        try
        {
            wanted = reader.kek().orElse(setting);
        }
        catch (RuntimeException failure)
        {
            LOG.warn("keys.kek_check_failed error={}",
                    failure.getClass().getSimpleName());
            return;
        }
        if (wanted.equals(current))
        {
            return;
        }
        try
        {
            probe.probe(wanted);
        }
        catch (Exception failure)
        {
            if (!wanted.equals(refused))
            {
                refused = wanted;
                LOG.warn("keys.kek_refused scope={} error={}", scope,
                        failure.getClass().getSimpleName());
                Telemetry.get().auditWarn("keys.kek_refused",
                        Fields.of(Attr.CONTROL_SCOPE, scope)
                                .with(Attr.KEK, wanted)
                                .with(Attr.REASON,
                                        failure.getClass().getSimpleName()));
            }
            return;
        }
        refused = null;
        current = wanted;
        LOG.info("keys.kek_active scope={} kek={}", scope, wanted);
        Telemetry.get().audit("keys.kek_active", Fields.of(
                Attr.CONTROL_SCOPE, scope).with(Attr.KEK, wanted));
    }

    /** Stops reading. */
    @Override
    public synchronized void close()
    {
        if (scheduler != null)
        {
            scheduler.shutdownNow();
        }
    }
}
