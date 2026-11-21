/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The only attribute keys telemetry may carry (design §12): identifiers,
 * codes, offsets and counts, never content, filenames or exception messages.
 * {@link Fields} accepts nothing else, and the application's exporters drop
 * any other key that reaches them from elsewhere.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum Attr
{

    /** The item's format, such as {@code ccda} or {@code x12}. */
    FORMAT("sdh.format"),

    /** The state of a file's checkpoint, such as {@code ARCHIVING}. */
    STATE("sdh.state"),

    /** The state a failure was changed from (STAGES.md §15.3). */
    FROM_STATE("sdh.from_state"),

    /** An operator's correction's number for its failure (§15.6c). */
    CORRECTION("sdh.correction"),

    /**
     * The SHA-256 of the public key an export was encrypted to, hex: who
     * can read it, without naming them (STAGES.md §15.6c).
     */
    RECIPIENT_KEY("sdh.recipient_key"),

    /**
     * The stage of processing, such as {@code intake}, {@code split} or
     * {@code archive}.
     */
    STAGE("sdh.stage"),

    /**
     * A step of a stage's work that is timed, such as {@code process},
     * {@code commit} or {@code send}.
     */
    PHASE("sdh.phase"),

    /** An {@link ErrorCode}. */
    ERROR_CODE("sdh.error_code"),

    /** The outcome of an operation, such as {@code ok} or {@code failed}. */
    RESULT("sdh.result"),

    /**
     * Why something happened, such as an exception's class name: never an
     * exception's message.
     */
    REASON("sdh.reason"),

    /**
     * A failure's chain of causes, by class name, outermost first, joined
     * by commas, such as {@code ExecutionException,IllegalStateException}
     * ({@link Fields#failure}): never a message.
     */
    CAUSES("sdh.causes"),

    /**
     * Where the innermost cause of a failure was thrown: its top frames,
     * each a class, method and line, joined by commas ({@link
     * Fields#failure}). Names code, never data.
     */
    THROWN_AT("sdh.thrown_at"),

    /**
     * The first frame of a failure in this product's own code, when the
     * innermost cause's top frames are a library's ({@link
     * Fields#failure}): a class, method and line. Names code, never data.
     */
    THROWN_FROM("sdh.thrown_from"),

    /**
     * A database failure's SQLSTATE, such as {@code 40P01} for a deadlock
     * ({@link Fields#failure}): the SQL standard's five-character class and
     * subclass, never the message.
     */
    SQL_STATE("sdh.sql_state"),

    /** A record's sequence number in its file, from 0. */
    SEQ("sdh.seq"),

    /** A byte offset in the file. */
    OFFSET("sdh.offset"),

    /** How many of something, such as records or messages. */
    COUNT("sdh.count"),

    /** A size in bytes. */
    BYTES("sdh.bytes"),

    /** The lease epoch, which goes up each time an instance takes the lease. */
    EPOCH("sdh.epoch"),

    /** The name of an Iceberg table. */
    TABLE("sdh.table"),

    /** Where a file was archived, such as {@code FAILED_FILES}. */
    ARCHIVE("sdh.archive"),

    /**
     * Who ran an operator command (design §11.4): an account name, never a
     * person's details.
     */
    OPERATOR("sdh.operator"),

    /**
     * A byte position within a record, such as where conversion failed
     * (design §6.4).
     */
    POSITION("sdh.position"),

    /**
     * The name of a landing intake (design §5.3), such as
     * {@code claims-sftp}: chosen in configuration, never a path or host.
     */
    INTAKE("sdh.intake"),

    /** An item's ID, a UUID: never its name or path. */
    ITEM_ID("sdh.item_id"),

    /**
     * Where an item came from, as its keyed hash ({@code SourceRef#hash},
     * design §5.3.2), which the landing tables already hold in the clear:
     * two items with the same one came from the same path, object key or
     * message ID. Never the reference itself.
     */
    SOURCE_REF("sdh.source_ref"),

    /**
     * Which sighting of its source an item was, as the ledger keys it
     * ({@code SourceRef#key}): a hash of the reference hash, size and
     * version, in hex. Two items from the same source with different ones
     * came from different versions, such as an object written twice.
     */
    SIGHTING("sdh.sighting"),

    /** What kind of item was landed, such as {@code file}. */
    KIND("sdh.kind"),

    /**
     * An Iceberg snapshot's ID, a number: of a table an intake reads
     * (design §5.3.18), never its data or file paths.
     */
    SNAPSHOT_ID("sdh.snapshot_id"),

    /** A message's type, such as {@code item.result}. */
    MESSAGE_TYPE("sdh.message_type"),

    /**
     * The version of what decides a stage's output beyond the record
     * (STAGES.md §6), as the stage running now has it: build, mapping and
     * a digest of the settings, never the settings themselves.
     */
    VERSION("sdh.version"),

    /** The version a record's stored output was made with. */
    STORED_VERSION("sdh.stored_version"),

    /**
     * Where an object stands in its line of processing (STAGES.md
     * §15.8.2), such as {@code g2 r1.3}: numbers, never content.
     */
    LINEAGE("sdh.lineage"),

    /**
     * The lineage, or generation such as {@code g2}, that superseded an
     * object (STAGES.md §15.8.3).
     */
    SUPERSEDED_BY("sdh.superseded_by"),

    /** An item's generation, from 1 (STAGES.md §15.8). */
    GENERATION("sdh.generation"),

    /** A reprocess run's ID (STAGES.md §15.8.4). */
    RUN_ID("sdh.run_id"),

    /**
     * The ID of an item that is an exact copy of an earlier one (STAGES.md
     * §15.5a), a UUID.
     */
    COPY_ID("sdh.copy_id"),

    /**
     * Why an object was made again (STAGES.md §15.8.7): {@code reprocess}
     * or {@code correction}.
     */
    CAUSE("sdh.cause"),

    /**
     * What a reprocess run chose by: {@code item}, {@code record} or
     * {@code made_by}; never the values given.
     */
    SELECTOR("sdh.selector"),

    /** What came of an object reprocessed: {@code stored} or {@code failed}. */
    OUTCOME("sdh.outcome"),

    /**
     * Whether a reprocess's output is the same as what it supersedes, by
     * digest (STAGES.md §15.8.2).
     */
    SAME_OUTPUT("sdh.same_output"),

    /** How many workers have claimed a record. */
    ATTEMPTS("sdh.attempts"),

    /** How many of an item's records failed, and were stored as they were. */
    FAILED_COUNT("sdh.failed_count"),

    /**
     * How many of an item's failures an operator declined to correct
     * (STAGES.md §15.3).
     */
    DECLINED_COUNT("sdh.declined_count"),

    /** How many of an item's failures are open: neither passed nor declined. */
    OPEN_COUNT("sdh.open_count"),

    /** How many of a reprocess run's objects were refused (§15.8.4). */
    REFUSED_COUNT("sdh.refused_count"),

    /**
     * The broker's ID of a received message, such as
     * {@code topic/partition/offset} on Kafka: where it is on the queue,
     * never its content.
     */
    BROKER_MESSAGE_ID("sdh.broker_message_id"),

    /**
     * What an operator asked of an item, such as {@code RESUME} for a held
     * split item (STAGES.md §15.6e).
     */
    ACTION("sdh.action"),

    /**
     * A stage's release, the digest of the versions its pod stamps
     * (STAGES.md §14.4a): never a version's settings.
     */
    RELEASE("sdh.release"),

    /** How a stage's release was let in, such as {@code drained}. */
    LET_IN_BY("sdh.let_in_by"),

    /** How many versions something names, a number. */
    VERSION_COUNT("sdh.version_count"),

    /**
     * The compliance profile a node started under (docs/HIPAA.md §8.1),
     * such as {@code hipaa}.
     */
    PROFILE("sdh.profile"),

    /**
     * A compliance profile's check, such as {@code transport-plaintext}.
     */
    CHECK("sdh.check"),

    /**
     * The name of a setting, such as {@code sdh.crypto.wrap-provider}, or
     * of a JVM option: never its value.
     */
    SETTING("sdh.setting"),

    /**
     * Why an operator waived a compliance check, as configured: text about
     * the deployment, never about documents.
     */
    WAIVER_REASON("sdh.waiver_reason"),

    /**
     * A record's ID (design §7.3): keyed over its content, so it names the
     * record without revealing it.
     */
    RECORD_ID("sdh.record_id"),

    /**
     * Who read a stored record outside the stages (HIPAA.md §4.3): the
     * name a reader is built with, such as a service account, never a
     * person's details.
     */
    READER("sdh.reader"),

    /**
     * Why a reader reads, as it is configured (HIPAA.md §4.3): a code such
     * as {@code TREAT}, {@code HPAYMT} or {@code PUBHLTH}, never free text
     * about a patient.
     */
    PURPOSE("sdh.purpose"),

    /**
     * The ticket an operator names for a step that needs one, such as
     * restoring an item in plaintext: a reference, never free text.
     */
    TICKET("sdh.ticket"),

    /** An audit chain's ID (HIPAA.md §3.1, audit controls). */
    CHAIN_ID("sdh.chain_id"),

    /**
     * How an operator's name was proved (CONTROL-API-DESIGN.md §10):
     * {@code jwt} or {@code mtls} through the control API, {@code cli}
     * when it was given to the CLI.
     */
    AUTH("sdh.auth"),

    /** The permission a control API call needed, such as pipeline.view. */
    PERMISSION("sdh.permission"),

    /**
     * Whether a control API call was let through: {@code allowed},
     * {@code denied} or {@code unauthenticated}.
     */
    DECISION("sdh.decision"),

    /** A control API request's own ID, which its answer names too. */
    REQUEST_ID("sdh.request_id"),

    /**
     * Why a control API action was asked for, as its caller gave it:
     * checked to be letters, digits and a few marks, so it can't hold a
     * document's text.
     */
    ACTION_REASON("sdh.action_reason"),

    /** A long operation's ID in the control API's record. */
    OPERATION_ID("sdh.operation_id"),

    /**
     * Whether a control API action was answered from its first answer,
     * a retry with the same idempotency key.
     */
    IDEMPOTENT_REPLAY("sdh.idempotent_replay");

    /**
     * The keys telemetry may carry: those of the attributes above, and
     * {@code event.name}, which OpenTelemetry uses to name events.
     */
    public static final Set<String> ALLOWED_KEYS = allowedKeys();

    /** The attribute's key, as exporters see it. */
    private final String key;

    /**
     * Creates an attribute.
     *
     * @param key the attribute's key
     */
    Attr(String key)
    {
        this.key = key;
    }

    /**
     * The attribute's key, as exporters see it.
     *
     * @return the key, such as {@code sdh.item_id}
     */
    public String key()
    {
        return key;
    }

    /**
     * Collects the allowed keys.
     *
     * @return every attribute's key, and {@code event.name}
     */
    private static Set<String> allowedKeys()
    {
        Set<String> keys = Arrays.stream(values()).map(Attr::key)
                .collect(Collectors.toCollection(HashSet::new));
        keys.add("event.name");
        return Set.copyOf(keys);
    }
}
