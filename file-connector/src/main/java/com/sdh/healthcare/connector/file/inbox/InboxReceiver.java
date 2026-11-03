/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.core.inbox.Inbox;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;

/**
 * Takes {@code item.landed} messages off the queue into the inbox (design
 * §5.3.6). A message is opened and checked: it must be {@code item.landed},
 * sealed to the splitter and signed by the landing node, for the splitter's
 * intake, and point at the landing table it reads. One that fails a check
 * is refused, audited and dropped: delivering it again won't change it.
 * <p>
 * The caller acknowledges the message once {@link #receive} returns. If the
 * inbox can't be written, it throws, and the message stays on the queue to
 * be delivered again.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboxReceiver
{

    /** What became of a message. */
    public enum Result
    {
        /** The item was added to the inbox. */
        ADDED,

        /** The item was already in the inbox. */
        REPEAT,

        /** The message was refused and dropped. */
        REJECTED
    }

    /** Where items wait to be split. */
    private final Inbox inbox;

    /** Decrypts and verifies messages. */
    private final MessageOpener opener;

    /** The intake the splitter serves. */
    private final String intake;

    /** The landing catalog's name. */
    private final String catalogName;

    /** The landing namespace. */
    private final String namespace;

    /**
     * Creates a receiver.
     *
     * @param inbox       where items wait to be split
     * @param opener      decrypts and verifies messages
     * @param intake      the intake the splitter serves
     * @param catalogName the landing catalog's name
     * @param namespace   the landing namespace
     */
    public InboxReceiver(Inbox inbox, MessageOpener opener, String intake,
            String catalogName, String namespace)
    {
        this.inbox = inbox;
        this.opener = opener;
        this.intake = intake;
        this.catalogName = catalogName;
        this.namespace = namespace;
    }

    /**
     * Opens, checks and records a message.
     *
     * @param body the message as received
     * @return what became of it
     * @throws Exception if the inbox can't be written; the message is to be
     *                   delivered again
     */
    public Result receive(byte[] body) throws Exception
    {
        ConnectorMessage message;
        try
        {
            message = opener.open(body);
        }
        catch (MessageRejectedException rejected)
        {
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
        return inbox.add(landed) ? Result.ADDED : Result.REPEAT;
    }

    /**
     * Audits a refused message.
     *
     * @param reason why it was refused, without PHI
     * @return {@link Result#REJECTED}
     */
    private Result reject(String reason)
    {
        Telemetry.get().auditError("inbox.rejected",
                Fields.of(Attr.ERROR_CODE, ErrorCode.MESSAGE_REJECTED)
                        .with(Attr.INTAKE, intake)
                        .with(Attr.REASON, reason));
        return Result.REJECTED;
    }
}
