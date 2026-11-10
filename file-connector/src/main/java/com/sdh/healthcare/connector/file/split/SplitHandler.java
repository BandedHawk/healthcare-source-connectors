/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.split;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException.Reason;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemReprocess;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.pipeline.ItemProcessor;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.stage.StageFailures;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The split stage's input (STAGES.md §8): takes each {@code item.landed}
 * message, checks it, and splits its item. A message must be
 * {@code item.landed}, sealed to the split stage and signed by the landing
 * node, for the stage's intake, and point at the landing table it reads.
 * Where the stage reprocesses, it takes {@code item.reprocess} too, signed
 * by the stage itself, and splits the item's next generation (STAGES.md
 * §15.8.4); an {@code item.landed} it signed, or an {@code item.reprocess}
 * another did, is refused ({@code wrong_signer}).
 * One that fails a check is refused, audited and acknowledged: delivering
 * it again won't change it. One of a contract major version this stage
 * can't read is held instead, and tried again, so that no item is
 * stranded by a landing node upgraded first.
 * <p>
 * A batch is acknowledged once each of its items is split. If one can't
 * be split now (storage or the broker down, another pod on the item, its
 * lease lost), the batch is held and tried again in place; items already
 * split in it are only announced again. So a batch of one message
 * ({@code sdh.stage.input.batch.max-messages: 1}) suits the stage best. A
 * tampered checkpoint, a commit whose outcome isn't known, or an
 * {@link Error} stops the process.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SplitHandler implements BatchHandler
{

    /** Logs retries, without PHI. */
    private static final Logger LOG =
            LoggerFactory.getLogger(SplitHandler.class);

    /** Splits the items. */
    private final ItemSplitter splitter;

    /** Decrypts and verifies messages. */
    private final MessageOpener opener;

    /** The intake the stage serves. */
    private final String intake;

    /** The landing catalog's name. */
    private final String catalogName;

    /** The landing namespace. */
    private final String namespace;

    /** Stops the process on a fatal error. */
    private final FatalExit fatal;

    /** The stage's reprocess; null if it doesn't reprocess. */
    private final SplitReprocess reprocess;

    /**
     * Creates a handler.
     *
     * @param splitter    splits the items
     * @param opener      decrypts and verifies messages
     * @param intake      the intake the stage serves
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param fatal       stops the process on a fatal error
     */
    public SplitHandler(ItemSplitter splitter, MessageOpener opener,
            String intake, String catalogName, String namespace,
            FatalExit fatal)
    {
        this(splitter, opener, intake, catalogName, namespace, fatal, null);
    }

    /**
     * Creates a handler that takes {@code item.reprocess} too (STAGES.md
     * §15.8.4).
     *
     * @param splitter    splits the items
     * @param opener      decrypts and verifies messages
     * @param intake      the intake the stage serves
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     * @param fatal       stops the process on a fatal error
     * @param reprocess   the stage's reprocess; null if it doesn't
     *                    reprocess
     */
    public SplitHandler(ItemSplitter splitter, MessageOpener opener,
            String intake, String catalogName, String namespace,
            FatalExit fatal, SplitReprocess reprocess)
    {
        this.reprocess = reprocess;
        this.splitter = splitter;
        this.opener = opener;
        this.intake = intake;
        this.catalogName = catalogName;
        this.namespace = namespace;
        this.fatal = fatal;
    }

    /**
     * Splits each message's item, in order.
     *
     * @param messages the messages
     * @return {@code DONE} once every item is split or its message
     *         refused; {@code RETRY} if one must be tried again;
     *         {@code FATAL} if the process is stopping
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public Outcome handle(List<BrokerMessage> messages)
            throws InterruptedException
    {
        for (BrokerMessage message : messages)
        {
            Outcome outcome = handle(message.body());
            if (outcome != Outcome.DONE)
            {
                return outcome;
            }
        }
        return Outcome.DONE;
    }

    /**
     * Opens, checks and splits one message's item.
     *
     * @param body the message as received
     * @return what became of it
     * @throws InterruptedException if interrupted while waiting
     */
    Outcome handle(byte[] body) throws InterruptedException
    {
        MessageOpener.Opened opened;
        try
        {
            opened = opener.openSigned(body);
        }
        catch (MessageRejectedException rejected)
        {
            if (rejected.reason() == Reason.UNSUPPORTED_VERSION)
            {
                // A later version: dropping it would strand the item; it
                // is held until this stage is upgraded. An earlier one,
                // which no upgrade reads, is rejected below, and its item
                // found by the reconciler.
                Telemetry.get().auditError("split.unreadable_version",
                        Fields.of(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                                .with(Attr.INTAKE, intake)
                                .with(Attr.REASON, rejected.reason().name()));
                return Outcome.RETRY;
            }
            return reject(rejected.reason().name());
        }
        ConnectorMessage message = opened.message();
        boolean self = reprocess != null
                && reprocess.selfKid().equals(opened.signerKid());
        if (message instanceof ItemReprocess again)
        {
            // Only the stage itself asks for a reprocess, and an item lands
            // only from another (STAGES.md §15.8.4).
            if (!self)
            {
                return wrongSigner(message);
            }
            if (!intake.equals(again.landed().intake()))
            {
                return reject("OTHER_INTAKE");
            }
            if (!checked(again.landed()))
            {
                return reject("OTHER_STORAGE");
            }
            return split(() -> splitter.reprocess(again, reprocess));
        }
        if (!(message instanceof ItemLanded landed))
        {
            return reject("NOT_ITEM_LANDED");
        }
        if (self)
        {
            return wrongSigner(message);
        }
        if (!intake.equals(landed.intake()))
        {
            return reject("OTHER_INTAKE");
        }
        if (!checked(landed))
        {
            return reject("OTHER_STORAGE");
        }
        return split(() -> splitter.split(landed.itemId(),
                landed.itemDigest(), landed.sameAs()));
    }

    /**
     * Whether an item's message points at the landing table this stage
     * reads. Its intake is checked by the caller.
     *
     * @param landed the item's message
     * @return whether its storage is the stage's landing table
     */
    private boolean checked(ItemLanded landed)
    {
        StorageRef storage = landed.storage();
        return storage != null && catalogName.equals(storage.catalog())
                && namespace.equals(storage.namespace())
                && LandingTables.LANDED_ITEMS.equals(storage.table());
    }

    /** Splitting an item, as a message asks. */
    @FunctionalInterface
    private interface Split
    {

        /**
         * Does it.
         *
         * @throws Exception if it fails
         */
        void run() throws Exception;
    }

    /**
     * Splits an item, and says what becomes of its message.
     *
     * @param work the splitting
     * @return {@code DONE}, {@code RETRY} or {@code FATAL}
     * @throws InterruptedException if interrupted while waiting
     */
    private Outcome split(Split work) throws InterruptedException
    {
        try
        {
            work.run();
            return Outcome.DONE;
        }
        catch (InterruptedException interrupted)
        {
            throw interrupted;
        }
        catch (LeaseHeldException held)
        {
            LOG.info("split.item_busy");
            return Outcome.RETRY;
        }
        catch (Exception | Error failure)
        {
            Optional<String> reason = fatalReason(failure);
            if (reason.isPresent())
            {
                LOG.error("split.fatal reason={}", reason.get());
                fatal.exit(reason.get());
                return Outcome.FATAL;
            }
            LOG.warn("split.retry error={}",
                    failure.getClass().getSimpleName());
            return Outcome.RETRY;
        }
    }

    /**
     * Tells whether an error must stop the process, and why
     * ({@link StageFailures#fatalReason}). A lost item lease is not one:
     * only that item stops.
     *
     * @param failure the error
     * @return the reason, for the exit; empty if the error can be retried
     */
    public static Optional<String> fatalReason(Throwable failure)
    {
        return StageFailures.fatalReason(failure);
    }

    /**
     * Audits a message refused for its signer, as the other stages do.
     *
     * @param message the message
     * @return {@code DONE}: it is acknowledged
     */
    private Outcome wrongSigner(ConnectorMessage message)
    {
        Telemetry.get().auditError("stage.message_rejected",
                Fields.of(Attr.STAGE, ItemProcessor.STAGE)
                        .with(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                        .with(Attr.INTAKE, intake)
                        .with(Attr.REASON, "wrong_signer")
                        .with(Attr.MESSAGE_TYPE, message.type().wireName()));
        return Outcome.DONE;
    }

    /**
     * Audits a refused message.
     *
     * @param reason why it was refused, without PHI
     * @return {@code DONE}: it is acknowledged
     */
    private Outcome reject(String reason)
    {
        Telemetry.get().auditError("split.rejected",
                Fields.of(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                        .with(Attr.INTAKE, intake)
                        .with(Attr.REASON, reason));
        return Outcome.DONE;
    }
}
