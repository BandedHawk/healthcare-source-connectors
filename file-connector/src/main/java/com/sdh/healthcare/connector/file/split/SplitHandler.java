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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.checkpoint.LeaseHeldException;
import com.sdh.healthcare.connector.core.landing.LandingTables;
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
        ConnectorMessage message;
        try
        {
            message = opener.open(body);
        }
        catch (MessageRejectedException rejected)
        {
            if (rejected.reason() == Reason.UNSUPPORTED_VERSION)
            {
                // Dropping it would strand the item; it is held until
                // this stage is upgraded.
                Telemetry.get().auditError("split.unreadable_version",
                        Fields.of(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                                .with(Attr.INTAKE, intake)
                                .with(Attr.REASON, rejected.reason().name()));
                return Outcome.RETRY;
            }
            return reject(rejected.reason().name());
        }
        if (!(message instanceof ItemLanded landed))
        {
            return reject("NOT_ITEM_LANDED");
        }
        if (!intake.equals(landed.intake()))
        {
            return reject("OTHER_INTAKE");
        }
        StorageRef storage = landed.storage();
        if (storage == null || !catalogName.equals(storage.catalog())
                || !namespace.equals(storage.namespace())
                || !LandingTables.LANDED_ITEMS.equals(storage.table()))
        {
            return reject("OTHER_STORAGE");
        }
        return split(landed);
    }

    /**
     * Splits an item, and says what becomes of its message.
     *
     * @param landed the item's message
     * @return {@code DONE}, {@code RETRY} or {@code FATAL}
     * @throws InterruptedException if interrupted while waiting
     */
    private Outcome split(ItemLanded landed) throws InterruptedException
    {
        try
        {
            splitter.split(landed.itemId(), landed.itemDigest());
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
