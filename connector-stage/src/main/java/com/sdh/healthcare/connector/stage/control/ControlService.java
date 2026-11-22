/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import com.sdh.healthcare.connector.stage.admin.Answering;
import com.sdh.healthcare.connector.stage.admin.Answers;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.Caller;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.ExportSealer;
import com.sdh.healthcare.connector.stage.admin.Followed;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Operation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import com.sdh.healthcare.connector.stage.admin.SupportCommands;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

/**
 * Serves the control API's operations (CONTROL-API-DESIGN.md §4, §5, §8,
 * §10): each request is bound into the operation's own request as the CLI
 * binds it, checked against the caller's permissions, run, answered, and
 * audited as {@code sdh.audit.control.<operation>} with the caller's
 * authenticated principal, whether it was allowed or not.
 *
 * <p>An action needs an {@code Idempotency-Key} and a {@code reason}. Its
 * first answer is kept for the key, so a retry with the same key and body
 * gets it back and nothing is done twice. A long operation the stage's
 * workers carry out ({@link Followed}) is recorded, answered {@code 202}
 * with where its record is, and followed from the stage whenever its record
 * is read.
 *
 * <p>An action whose operation needs a second person
 * (CONTROL-API-DESIGN.md §6) isn't carried out: it is recorded as
 * {@code awaiting-approval}, and answered {@code 202}. Another principal
 * with the same permissions approves or rejects it; its requester may
 * cancel it. Once approved, a control pod's {@link ControlRunner} runs it.
 * A break-glass result is held encrypted ({@link ControlResults}) for its
 * requester to fetch once.
 *
 * <p>Nothing a caller didn't send is ever put in an answer or an event
 * besides the operation's own answer: a failure is answered and audited by
 * its class alone, never its message, which could quote stored data.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlService implements AutoCloseable
{

    /** The authorities a caller's roles are given as, before the name. */
    public static final String ROLE = "role:";

    /** A JSON answer's media type. */
    static final String JSON = "application/json";

    /** A refusal's media type (RFC 9457). */
    static final String PROBLEM = "application/problem+json";

    /** Streamed rows' media type. */
    static final String NDJSON = "application/x-ndjson";

    /** The largest body an action takes: a corrected record, base64. */
    public static final int MAX_BODY = 16 * 1024 * 1024;

    /** The largest body a decision takes: a reason. */
    public static final int MAX_DECISION = 4096;

    /** The most operations listed at once. */
    static final int MAX_LIMIT = 1000;

    /** How many operations are listed unless asked. */
    static final int DEFAULT_LIMIT = 100;

    /** The operations' path. */
    static final String OPERATIONS = ControlRoute.PREFIX + "/operations";

    /** A waiting operation's parameter: the route it was asked by. */
    private static final String ROUTE = "route";

    /** A waiting operation's parameter: its path's variables. */
    private static final String VARIABLES = "variables";

    /** A waiting operation's parameter: its body's fields, but reason. */
    private static final String FIELDS = "fields";

    /** A waiting operation's parameter: the permissions it needs. */
    private static final String PERMISSIONS = "permissions";

    /** A waiting operation's parameter: when it expires unapproved. */
    private static final String APPROVE_BY = "approveBy";

    /** The most identifiers a patient lookup takes. */
    static final int MAX_IDENTIFIERS = 1000;

    /** The longest identifier a patient lookup takes. */
    static final int MAX_IDENTIFIER = 512;

    /** A ticket: a reference to a change or incident, never free text. */
    private static final Pattern TICKET =
            Pattern.compile("[A-Za-z0-9._:/-]{1,64}");

    /** A control character, which no identifier holds. */
    private static final Pattern IDENTIFIER_CONTROL =
            Pattern.compile("\\p{Cntrl}");

    /** An idempotency key: opaque, of the client's choosing, no content. */
    private static final Pattern KEY =
            Pattern.compile("[A-Za-z0-9._:-]{8,128}");

    /**
     * A reason: letters, digits, spaces and a few marks, so it can't hold a
     * document's text (CONTROL-API-DESIGN.md §5).
     */
    private static final Pattern REASON =
            Pattern.compile("[A-Za-z0-9 .,:_#/-]{1,200}");

    /** The stage's operator commands. */
    private final AdminCli<Object, ?> cli;

    /** The stage's settings. */
    private final Object settings;

    /** The roles each permission is granted to. */
    private final Map<Permission, Set<String>> grants;

    /** How callers are authenticated, for the audit. */
    private final Caller.Auth auth;

    /** The idempotency keys and the operation records. */
    private final ControlStore store;

    /** The stage's operations' scope, {@code stage:<service>}. */
    private final String scope;

    /** The principals' sealing keys. */
    private final OperatorKeys keys;

    /** The control API's own operations, on the sealing keys, by name. */
    private final Map<String, CliOperation<OperatorKeys, ?, ?>> own;

    /** The break-glass results held for their requesters. */
    private final ControlResults results;

    /** How long a request may wait for its approval. */
    private final ControlSettings.Approval approval;

    /** The settings in force in this pod, for the support commands. */
    private final Map<String, String> effective;

    /** Runs approved operations; null until started. */
    private volatile ControlRunner runner;

    /**
     * A request, as it came.
     *
     * @param route      the route it came by
     * @param caller     the caller, authenticated
     * @param variables  the path's variables
     * @param parameters the query's parameters
     * @param key        the {@code Idempotency-Key}; null if not given
     * @param path       the request's path, for its hash
     * @param body       its body; empty for none
     */
    public record Call(ControlRoute route, Authentication caller,
            Map<String, String> variables, Map<String, String[]> parameters,
            String key, String path, byte[] body)
    {
    }

    /**
     * An answer, as it is sent and kept for a retry.
     *
     * @param status   the HTTP status
     * @param type     its media type
     * @param body     its body: JSON, which holds no PHI
     * @param location its {@code Location}; null for none
     */
    record Answer(int status, String type, String body, String location)
    {
    }

    /**
     * Creates the service.
     *
     * @param cli      the stage's operator commands
     * @param settings the stage's settings
     * @param control  the control API's settings
     * @param store    the idempotency keys and operation records
     * @param keys     the principals' sealing keys
     * @param results  the break-glass results
     */
    public ControlService(AdminCli<Object, ?> cli, Object settings,
            ControlSettings control, ControlStore store, OperatorKeys keys,
            ControlResults results)
    {
        this(cli, settings, control, store, keys, results, Map.of());
    }

    /**
     * Creates the service, with the settings in force for the support
     * commands (CONTROL-API-DESIGN.md §15.9).
     *
     * @param cli       the stage's operator commands
     * @param settings  the stage's settings
     * @param control   the control API's settings
     * @param store     the idempotency keys and operation records
     * @param keys      the principals' sealing keys
     * @param results   the break-glass results
     * @param effective the settings in force, by canonical name
     */
    public ControlService(AdminCli<Object, ?> cli, Object settings,
            ControlSettings control, ControlStore store, OperatorKeys keys,
            ControlResults results, Map<String, String> effective)
    {
        this.effective = Map.copyOf(effective);
        this.cli = cli;
        this.settings = settings;
        this.grants = control.grants();
        this.auth = control.auth().mode() == ControlSettings.Mode.JWT
                ? Caller.Auth.JWT : Caller.Auth.MTLS;
        this.store = store;
        this.scope = "stage:" + cli.stage().service();
        this.keys = keys;
        this.own = OperatorKeyCommands.all().stream().collect(
                Collectors.toUnmodifiableMap(Operation::name,
                        operation -> operation));
        this.results = results;
        this.approval = control.approval();
    }

    /** Starts running approved operations. */
    public void start()
    {
        ControlRunner started = new ControlRunner(store, results, scope,
                this::execute);
        runner = started;
        started.start();
    }

    /**
     * Opens what a stage's command reads from; a support command's with
     * the settings in force and the stage's recent operations.
     *
     * @param name the command's name
     * @return it, to close after the command
     */
    private AutoCloseable open(String name)
    {
        return cli.open(name, settings, effective, () -> store.list(scope,
                SupportCommands.OPERATIONS, null).stream()
                .map(ControlService::summary).toList());
    }

    /**
     * An operation as the support bundle holds it: what, where it stands,
     * who and when, and its counts; never its parameters or reasons.
     *
     * @param operation the operation
     * @return the summary
     */
    static Map<String, Object> summary(ControlStore.Operation operation)
    {
        return Answers.object("id", operation.id(), "kind",
                operation.kind(), "state", operation.state().wire(),
                "requestedBy", operation.requestedBy(),
                "requestedAt", operation.requestedAt(),
                "decidedBy", operation.decidedBy(),
                "ticket", operation.ticket(),
                "updatedAt", operation.updatedAt());
    }

    /** Stops running approved operations. */
    @Override
    public void close()
    {
        ControlRunner started = runner;
        if (started != null)
        {
            started.close();
        }
    }

    /**
     * The permissions a caller has, from its roles.
     *
     * @param caller the caller, authenticated
     * @return its permissions
     */
    public Set<Permission> granted(Authentication caller)
    {
        Set<String> roles = caller.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .filter(name -> name != null && name.startsWith(ROLE))
                .map(name -> name.substring(ROLE.length()))
                .collect(Collectors.toSet());
        Set<Permission> granted = EnumSet.noneOf(Permission.class);
        grants.forEach((permission, to) ->
        {
            if (to.stream().anyMatch(roles::contains))
            {
                granted.add(permission);
            }
        });
        return granted;
    }

    /**
     * Serves a request of an operation.
     *
     * @param call     the request
     * @param response where the answer goes
     * @throws IOException if the answer can't be written
     */
    @SuppressWarnings("unchecked")
    public void serve(Call call, HttpServletResponse response)
            throws IOException
    {
        ControlRoute route = call.route();
        String requestId = UUID.randomUUID().toString();
        Caller who = new Caller(call.caller().getName(), auth);
        AuditRecord audit = new AuditRecord(who.principal());
        audit.add(Attr.AUTH, auth.name().toLowerCase(Locale.ROOT));
        audit.add(Attr.REQUEST_ID, requestId);
        String event = "control." + route.operation().replace('-', '_');
        Optional<CliOperation<?, ?, ?>> found = route.own()
                ? Optional.ofNullable(own.get(route.operation()))
                : cli.operation(route.operation());
        if (found.isEmpty())
        {
            refuse(event, audit, "not-on-this-stage");
            send(response, problem(404, "not-on-this-stage", "This stage"
                    + " has no " + route.operation() + " operation",
                    requestId), requestId);
            return;
        }
        CliOperation<Object, Object, Object> operation =
                (CliOperation<Object, Object, Object>) found.get();
        Body body;
        try
        {
            body = route.hasBody() ? body(call, audit) : new Body(
                    queryFields(call.parameters()), null, new byte[0],
                    null);
        }
        catch (AdminArgs.UsageException refused)
        {
            refuse(event, audit, "bad-request");
            send(response, problem(400, "bad-request", refused.getMessage(),
                    requestId), requestId);
            return;
        }
        Optional<byte[]> sealTo = Optional.empty();
        byte[] input = body.input();
        if (route.seals())
        {
            audit.add(Attr.SEALING_KEY, body.sealTo());
            try
            {
                sealTo = keys.usable(who.principal(), body.sealTo());
            }
            catch (RuntimeException unread)
            {
                Telemetry.get().auditWarn(event, audit.fields()
                        .with(Attr.RESULT, "failed")
                        .with(Attr.REASON, unread.getClass()
                                .getSimpleName()));
                send(response, problem(500, "failed",
                        unread.getClass().getSimpleName(), requestId),
                        requestId);
                return;
            }
            input = sealTo.map(pem -> join(pem, body.input()))
                    .orElse(new byte[0]);
        }
        Streamed rows = new Streamed(response, requestId, route.seals()
                ? Answers.object("operation", route.operation(),
                        "sealedTo", body.sealTo(), "requestId", requestId)
                : null);
        boolean claimed = false;
        try (AutoCloseable target = route.own() ? keys
                : open(route.operation()))
        {
            Object request;
            try
            {
                AdminArgs arguments = arguments(route, call.variables(),
                        body.fields()).withInput(new ByteArrayInputStream(
                                input));
                request = operation.bind(route.toCaller()
                        ? arguments.withOutputToCaller() : arguments,
                        target);
            }
            catch (AdminArgs.UsageException failure)
            {
                refuse(event, audit, "bad-request");
                send(response, problem(400, "bad-request",
                        failure.getMessage(), requestId), requestId);
                return;
            }
            Set<Permission> needed = operation.permissions(request);
            audit.add(Attr.PERMISSION, needed.stream().map(Permission::key)
                    .sorted().collect(Collectors.joining(",")));
            if (!granted(call.caller()).containsAll(needed))
            {
                refuse(event, audit, "denied");
                send(response, problem(403, "permission-required", "This"
                        + " needs " + needed.stream().map(Permission::key)
                                .sorted().collect(Collectors.joining(
                                        " and ")), requestId), requestId);
                return;
            }
            boolean fourEyes = operation.approval(request)
                    == Operation.Approval.FOUR_EYES;
            if (fourEyes && !route.acts())
            {
                refuse(event, audit, "needs-approval");
                send(response, problem(403, "approval-required", "This"
                        + " needs a second person's approval", requestId),
                        requestId);
                return;
            }
            if (route.seals() && sealTo.isEmpty())
            {
                refuse(event, audit, "sealing-key-not-yours");
                send(response, problem(403, "sealing-key-not-yours", "Seal"
                        + " to one of your own registered keys, unexpired"
                        + " and not revoked", requestId), requestId);
                return;
            }
            if (route.acts())
            {
                ControlStore.Claim claim = store.claim(who.principal(),
                        call.key(), route.operation(), hash(call));
                switch (claim)
                {
                    case ControlStore.Mismatch mismatch ->
                    {
                        refuse(event, audit, "key-reused");
                        send(response, problem(409, "idempotency-key-reused",
                                "This key was used for another request",
                                requestId), requestId);
                        return;
                    }
                    case ControlStore.Pending pending ->
                    {
                        refuse(event, audit, "in-progress");
                        send(response, problem(409, "in-progress", "The"
                                + " request with this key is still being"
                                + " carried out", requestId), requestId);
                        return;
                    }
                    case ControlStore.Replay replay ->
                    {
                        Telemetry.get().audit(event, audit.fields()
                                .with(Attr.DECISION, "allowed")
                                .with(Attr.IDEMPOTENT_REPLAY, true)
                                .with(Attr.RESULT, "replayed"));
                        send(response, new Answer(replay.status(),
                                replay.contentType(), replay.body(),
                                replay.location()), requestId);
                        return;
                    }
                    case ControlStore.Fresh fresh -> claimed = true;
                }
            }
            audit.add(Attr.DECISION, "allowed");
            Invocation invocation = new Invocation(who, audit, rows);
            Answer answer;
            if (fourEyes)
            {
                answer = awaiting(route, call, body, needed, who, audit);
            }
            else if (route.acts()
                    && operation instanceof Followed<?, ?, ?> any)
            {
                Followed<Object, Object, Object> followed =
                        (Followed<Object, Object, Object>) any;
                Object result = followed.start(request, target, invocation);
                Optional<Map<String, Object>> started =
                        followed.started(result);
                if (started.isPresent())
                {
                    ControlStore.Operation recorded = store.record(
                            route.operation(), scope, started.get(),
                            who.principal(), body.reason(),
                            ControlStore.State.RUNNING, Map.of());
                    audit.add(Attr.OPERATION_ID, recorded.id());
                    answer = new Answer(202, JSON, json(answer(recorded,
                            null)), OPERATIONS + "/" + recorded.id());
                }
                else
                {
                    answer = answered(operation, result);
                }
            }
            else
            {
                Object result = operation.run(request, target, invocation);
                if (route.streamed())
                {
                    rows.finish();
                    if (route.seals())
                    {
                        audit.add(Attr.PARTS, rows.parts());
                    }
                    answer = null;
                }
                else
                {
                    answer = answered(operation, result);
                }
            }
            if (claimed)
            {
                store.answered(who.principal(), call.key(), answer.status(),
                        answer.type(), answer.body(), answer.location());
            }
            // Audited before the answer is sent, so a caller holding the
            // answer finds its event.
            Telemetry.get().audit(event,
                    audit.fields().with(Attr.RESULT, "ok"));
            if (answer != null)
            {
                send(response, answer, requestId);
            }
        }
        catch (Refusal | AdminArgs.UsageException refusal)
        {
            // A command's usage refusal at run time, such as a key too
            // weak, is the caller's to fix: 400, with the command's own
            // words, which name no stored data.
            int status = refusal instanceof Refusal own ? own.status() : 400;
            String code = refusal instanceof Refusal own ? own.code()
                    : "bad-request";
            refuse(event, audit, code);
            if (rows.started())
            {
                rows.broken(refusal);
                return;
            }
            Answer refused = problem(status, code, refusal.getMessage(),
                    requestId);
            if (claimed)
            {
                keep(who.principal(), call.key(), refused);
            }
            send(response, refused, requestId);
        }
        catch (Exception failure)
        {
            if (route.seals())
            {
                audit.add(Attr.PARTS, rows.parts());
            }
            Telemetry.get().auditWarn(event, audit.fields()
                    .with(Attr.RESULT, "failed")
                    .with(Attr.REASON, failure.getClass().getSimpleName()));
            if (rows.started())
            {
                rows.broken(failure);
                return;
            }
            Answer refused = failure instanceof IllegalStateException
                    ? problem(409, "refused",
                            failure.getClass().getSimpleName(), requestId)
                    : problem(500, "failed",
                            failure.getClass().getSimpleName(), requestId);
            if (claimed)
            {
                keep(who.principal(), call.key(), refused);
            }
            send(response, refused, requestId);
        }
    }

    /**
     * Serves the stage's operation records: one, or a page of them,
     * newest first. A record not finished is brought up to date from the
     * stage as it is read.
     *
     * @param caller     the caller, authenticated
     * @param id         the operation's ID, as given; null to list
     * @param parameters the query's parameters
     * @param response   where the answer goes
     * @throws IOException if the answer can't be written
     */
    public void operations(Authentication caller, String id,
            Map<String, String[]> parameters, HttpServletResponse response)
            throws IOException
    {
        String requestId = UUID.randomUUID().toString();
        AuditRecord audit = new AuditRecord(caller.getName());
        audit.add(Attr.AUTH, auth.name().toLowerCase(Locale.ROOT));
        audit.add(Attr.REQUEST_ID, requestId);
        audit.add(Attr.PERMISSION, Permission.PIPELINE_VIEW.key());
        String event = "control.operations";
        if (!granted(caller).contains(Permission.PIPELINE_VIEW))
        {
            refuse(event, audit, "denied");
            send(response, problem(403, "permission-required", "This needs "
                    + Permission.PIPELINE_VIEW.key(), requestId), requestId);
            return;
        }
        Answer answer;
        try
        {
            answer = id == null ? page(parameters, requestId)
                    : one(id, parameters, audit, requestId);
        }
        catch (AdminArgs.UsageException refused)
        {
            refuse(event, audit, "bad-request");
            send(response, problem(400, "bad-request", refused.getMessage(),
                    requestId), requestId);
            return;
        }
        catch (RuntimeException failure)
        {
            Telemetry.get().auditWarn(event, audit.fields()
                    .with(Attr.DECISION, "allowed")
                    .with(Attr.RESULT, "failed")
                    .with(Attr.REASON, failure.getClass().getSimpleName()));
            send(response, problem(500, "failed",
                    failure.getClass().getSimpleName(), requestId),
                    requestId);
            return;
        }
        Telemetry.get().audit(event, audit.fields()
                .with(Attr.DECISION, "allowed")
                .with(Attr.RESULT, answer.status() == 200 ? "ok"
                        : "not-found"));
        send(response, answer, requestId);
    }

    /**
     * Records an action that needs a second person, to wait for one.
     *
     * @param route  its route
     * @param call   the request
     * @param body   what its body gave
     * @param needed the permissions it needs, which its approver needs too
     * @param who    the requester
     * @param audit  the audit event
     * @return the answer: {@code 202}, with where its record is
     * @throws AdminArgs.UsageException if a break-glass request has no
     *         ticket, or a ticket isn't a reference
     */
    private Answer awaiting(ControlRoute route, Call call, Body body,
            Set<Permission> needed, Caller who, AuditRecord audit)
    {
        Object given = body.fields().get(ControlRoute.TICKET);
        String ticket = given instanceof String text ? text : null;
        if (given != null && (ticket == null
                || !TICKET.matcher(ticket).matches()))
        {
            throw new AdminArgs.UsageException("ticket must be a reference:"
                    + " letters, digits and . _ : / -, up to 64");
        }
        boolean breakGlass = needed.contains(Permission.PHI_BREAK_GLASS);
        if (breakGlass && ticket == null)
        {
            throw new AdminArgs.UsageException("A break-glass request needs"
                    + " a ticket: the change or incident it is for");
        }
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put(ROUTE, route.key());
        parameters.put(VARIABLES, new LinkedHashMap<>(call.variables()));
        parameters.put(FIELDS, new LinkedHashMap<>(body.fields()));
        parameters.put(PERMISSIONS, needed.stream().map(Permission::key)
                .sorted().toList());
        parameters.put(APPROVE_BY, Instant.now().plus(breakGlass
                ? approval.breakGlassTtl() : approval.ttl()).toString());
        ControlStore.Operation recorded = store.record(route.operation(),
                scope, parameters, who.principal(), body.reason(), ticket,
                ControlStore.State.AWAITING_APPROVAL, Map.of());
        audit.add(Attr.OPERATION_ID, recorded.id());
        audit.add(Attr.OUTCOME, ControlStore.State.AWAITING_APPROVAL.wire());
        if (ticket != null)
        {
            audit.add(Attr.TICKET, ticket);
        }
        return new Answer(202, JSON, json(answer(recorded, null)),
                OPERATIONS + "/" + recorded.id());
    }

    /** What a second person, or the requester, does to a request. */
    public enum Decision
    {

        /** Approves it, so it runs. */
        APPROVE,

        /** Rejects it. */
        REJECT,

        /** Cancels it: its requester, before it runs. */
        CANCEL;

        /**
         * Its name in paths and audit events.
         *
         * @return the name, such as {@code approve}
         */
        public String wire()
        {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Serves an approval, a rejection or a cancellation of an operation
     * that needs a second person. An approver or rejecter holds the
     * permissions the operation needs and isn't its requester; only the
     * requester cancels. Each needs a {@code reason}. A decision can be
     * made once: asking again is {@code 409} with the operation's state,
     * so it needs no {@code Idempotency-Key}.
     *
     * @param caller   the caller, authenticated
     * @param id       the operation's ID, as given
     * @param decision what is asked
     * @param body     the request's body
     * @param response where the answer goes
     * @throws IOException if the answer can't be written
     */
    public void decide(Authentication caller, String id, Decision decision,
            byte[] body, HttpServletResponse response) throws IOException
    {
        String requestId = UUID.randomUUID().toString();
        String principal = caller.getName();
        AuditRecord audit = new AuditRecord(principal);
        audit.add(Attr.AUTH, auth.name().toLowerCase(Locale.ROOT));
        audit.add(Attr.REQUEST_ID, requestId);
        String event = "control.operation_" + decision.wire();
        try
        {
            String reason = reasonOnly(body);
            audit.add(Attr.ACTION_REASON, reason);
            UUID operationId = uuid(id);
            audit.add(Attr.OPERATION_ID, operationId);
            Optional<ControlStore.Operation> found =
                    store.find(operationId, scope);
            if (found.isEmpty())
            {
                refuse(event, audit, "not-found");
                send(response, problem(404, "operation-not-found", "This"
                        + " stage has no such operation", requestId),
                        requestId);
                return;
            }
            ControlStore.Operation operation = expired(found.get());
            audit.add(Attr.REQUESTED_BY, operation.requestedBy());
            Set<Permission> needed = needed(operation);
            audit.add(Attr.PERMISSION, needed.stream().map(Permission::key)
                    .sorted().collect(Collectors.joining(",")));
            boolean requester = operation.requestedBy()
                    .equalsIgnoreCase(principal);
            Answer refused = null;
            if (needed.isEmpty())
            {
                refused = problem(409, "wrong-state", "This operation needs"
                        + " no approval", requestId);
            }
            else if (decision == Decision.CANCEL ? !requester
                    : requester)
            {
                refused = decision == Decision.CANCEL
                        ? problem(403, "not-your-request", "Only its"
                                + " requester cancels a request", requestId)
                        : problem(403, "own-request", "A request is"
                                + " approved or rejected by someone other"
                                + " than its requester", requestId);
            }
            else if (decision != Decision.CANCEL
                    && !granted(caller).containsAll(needed))
            {
                refused = problem(403, "permission-required", "This needs "
                        + needed.stream().map(Permission::key).sorted()
                                .collect(Collectors.joining(" and ")),
                        requestId);
            }
            else if (!(decision == Decision.CANCEL
                    ? store.cancel(operationId)
                    : store.decide(operationId,
                            decision == Decision.APPROVE, principal,
                            reason)))
            {
                ControlStore.Operation now = store.find(operationId, scope)
                        .orElse(operation);
                refused = problem(409, "wrong-state", "The operation is "
                        + now.state().wire(), requestId);
            }
            if (refused != null)
            {
                refuse(event, audit, title(refused));
                send(response, refused, requestId);
                return;
            }
            ControlStore.Operation decided = store.find(operationId, scope)
                    .orElseThrow();
            Telemetry.get().audit(event, audit.fields()
                    .with(Attr.DECISION, "allowed")
                    .with(Attr.RESULT, "ok"));
            send(response, new Answer(200, JSON, json(answer(decided, null)),
                    null), requestId);
            ControlRunner started = runner;
            if (decision == Decision.APPROVE && started != null)
            {
                started.nudge();
            }
        }
        catch (AdminArgs.UsageException wrong)
        {
            refuse(event, audit, "bad-request");
            send(response, problem(400, "bad-request", wrong.getMessage(),
                    requestId), requestId);
        }
        catch (RuntimeException failure)
        {
            Telemetry.get().auditWarn(event, audit.fields()
                    .with(Attr.RESULT, "failed")
                    .with(Attr.REASON, failure.getClass().getSimpleName()));
            send(response, problem(500, "failed",
                    failure.getClass().getSimpleName(), requestId),
                    requestId);
        }
    }

    /**
     * Serves a break-glass result to its requester, once: decrypted, as
     * {@code application/octet-stream}, then deleted. A result fetched
     * already, past its time, or never held is {@code 410}.
     *
     * @param caller   the caller, authenticated
     * @param id       the operation's ID, as given
     * @param response where the answer goes
     * @throws IOException if the answer can't be written
     */
    public void result(Authentication caller, String id,
            HttpServletResponse response) throws IOException
    {
        String requestId = UUID.randomUUID().toString();
        String principal = caller.getName();
        AuditRecord audit = new AuditRecord(principal);
        audit.add(Attr.AUTH, auth.name().toLowerCase(Locale.ROOT));
        audit.add(Attr.REQUEST_ID, requestId);
        audit.add(Attr.PERMISSION, Permission.PHI_BREAK_GLASS.key());
        String event = "control.operation_result";
        boolean started = false;
        try
        {
            if (!granted(caller).contains(Permission.PHI_BREAK_GLASS))
            {
                refuse(event, audit, "denied");
                send(response, problem(403, "permission-required", "This"
                        + " needs " + Permission.PHI_BREAK_GLASS.key(),
                        requestId), requestId);
                return;
            }
            UUID operationId = uuid(id);
            audit.add(Attr.OPERATION_ID, operationId);
            Optional<ControlStore.Operation> found =
                    store.find(operationId, scope);
            if (found.isEmpty())
            {
                refuse(event, audit, "not-found");
                send(response, problem(404, "operation-not-found", "This"
                        + " stage has no such operation", requestId),
                        requestId);
                return;
            }
            if (!found.get().requestedBy().equalsIgnoreCase(principal))
            {
                refuse(event, audit, "not-your-request");
                send(response, problem(403, "not-your-request", "Only its"
                        + " requester fetches a result", requestId),
                        requestId);
                return;
            }
            Optional<ControlResults.Claimed> claimed =
                    found.get().state() == ControlStore.State.DONE
                            ? results.claim(operationId) : Optional.empty();
            if (claimed.isEmpty())
            {
                refuse(event, audit, "result-gone");
                send(response, problem(410, "result-gone", "There is no"
                        + " result to fetch: it was fetched already, is"
                        + " past its time, or the operation has none",
                        requestId), requestId);
                return;
            }
            audit.add(Attr.PARTS, claimed.get().parts());
            response.setStatus(200);
            response.setContentType("application/octet-stream");
            response.setHeader("Cache-Control", "no-store");
            response.setHeader("X-Request-Id", requestId);
            response.setContentLengthLong(claimed.get().bytes());
            started = true;
            long written = results.write(claimed.get(),
                    response.getOutputStream());
            audit.add(Attr.BYTES, written);
            Telemetry.get().audit(event, audit.fields()
                    .with(Attr.DECISION, "allowed")
                    .with(Attr.RESULT, "ok"));
        }
        catch (AdminArgs.UsageException wrong)
        {
            refuse(event, audit, "bad-request");
            send(response, problem(400, "bad-request", wrong.getMessage(),
                    requestId), requestId);
        }
        catch (IOException | RuntimeException failure)
        {
            Telemetry.get().auditWarn(event, audit.fields()
                    .with(Attr.DECISION, "allowed")
                    .with(Attr.RESULT, "failed")
                    .with(Attr.REASON, failure.getClass().getSimpleName()));
            if (!started)
            {
                send(response, problem(500, "failed",
                        failure.getClass().getSimpleName(), requestId),
                        requestId);
            }
        }
    }

    /**
     * Runs an approved operation, as its runner's lease holder, and
     * finishes its record. It is audited as the operation, with its
     * requester as the operator and its approver beside. A plaintext
     * result is held encrypted for the requester; if the run fails, what
     * was held of it is deleted.
     *
     * @param operation its record
     * @param holder    the lease's holder
     */
    @SuppressWarnings("unchecked")
    void execute(ControlStore.Operation operation, String holder)
    {
        AuditRecord audit = new AuditRecord(operation.requestedBy());
        audit.add(Attr.AUTH, auth.name().toLowerCase(Locale.ROOT));
        audit.add(Attr.REQUEST_ID, UUID.randomUUID().toString());
        audit.add(Attr.OPERATION_ID, operation.id());
        audit.add(Attr.REQUESTED_BY, operation.requestedBy());
        audit.add(Attr.APPROVED_BY, operation.decidedBy());
        audit.add(Attr.ACTION_REASON, operation.reason());
        if (operation.ticket() != null)
        {
            audit.add(Attr.TICKET, operation.ticket());
        }
        audit.add(Attr.DECISION, "approved");
        String event = "control." + operation.kind().replace('-', '_');
        ControlResults.Holding[] holding = new ControlResults.Holding[1];
        try
        {
            Map<String, Object> parameters = operation.parameters();
            ControlRoute route = ControlRoute.all().stream()
                    .filter(each -> each.key().equals(
                            parameters.get(ROUTE)))
                    .findFirst().orElseThrow(() -> new IllegalStateException(
                            "The operation's route is not served"));
            CliOperation<Object, Object, Object> command =
                    (CliOperation<Object, Object, Object>) cli.operation(
                            operation.kind()).orElseThrow(
                                    () -> new IllegalStateException("The"
                                            + " stage has no such operation"));
            Map<String, String> variables = new LinkedHashMap<>();
            ((Map<String, Object>) parameters.getOrDefault(VARIABLES,
                    Map.of())).forEach((name, value) ->
                            variables.put(name, String.valueOf(value)));
            Map<String, Object> fields = new LinkedHashMap<>(
                    (Map<String, Object>) parameters.getOrDefault(FIELDS,
                            Map.of()));
            Invocation.Sink sink = new Invocation.Sink()
            {
                @Override
                public void row(Map<String, Object> row)
                {
                    throw new UnsupportedOperationException("An approved"
                            + " operation's result is held, not streamed");
                }

                @Override
                public void sealed(String jwe)
                {
                    throw new UnsupportedOperationException("An approved"
                            + " operation's result is held, not sealed");
                }

                @Override
                public void plaintext(byte[] part)
                {
                    if (holding[0] == null)
                    {
                        holding[0] = results.hold(operation.id());
                    }
                    holding[0].part(part);
                }
            };
            Object result;
            try (AutoCloseable target = open(operation.kind()))
            {
                AdminArgs arguments = arguments(route, variables, fields)
                        .withInput(new ByteArrayInputStream(new byte[0]));
                Object request = command.bind(route.toCaller()
                        ? arguments.withOutputToCaller() : arguments,
                        target);
                result = command.run(request, target, new Invocation(
                        new Caller(operation.requestedBy(), auth), audit,
                        sink));
            }
            Map<String, Object> done = new LinkedHashMap<>(
                    command instanceof Answering<?> answering
                            ? ((Answering<Object>) answering).answer(result)
                            : Map.of());
            if (holding[0] != null)
            {
                Instant until = holding[0].finish(approval.breakGlassTtl());
                audit.add(Attr.PARTS, holding[0].parts());
                done.put("resultParts", holding[0].parts());
                done.put("resultBytes", holding[0].bytes());
                done.put("resultUntil", until.toString());
                done.put("resultAt", OPERATIONS + "/" + operation.id()
                        + "/result");
            }
            store.finish(operation.id(), holder, ControlStore.State.DONE,
                    operation.progress(), done);
            Telemetry.get().audit(event,
                    audit.fields().with(Attr.RESULT, "ok"));
        }
        catch (Exception failure)
        {
            String reason = failure.getClass().getSimpleName();
            Telemetry.get().auditWarn(event, audit.fields()
                    .with(Attr.RESULT, "failed")
                    .with(Attr.REASON, reason));
            try
            {
                if (holding[0] != null)
                {
                    holding[0].discard();
                }
                store.finish(operation.id(), holder,
                        ControlStore.State.FAILED, operation.progress(),
                        Answers.object("failure", reason));
            }
            catch (RuntimeException unrecorded)
            {
                // Its lease runs out, and another pod runs it again.
            }
        }
    }

    /**
     * The permissions an operation waiting for a second person needs, as
     * it was recorded.
     *
     * @param operation its record
     * @return them; empty if it needs no approval
     */
    private static Set<Permission> needed(ControlStore.Operation operation)
    {
        Object listed = operation.parameters().get(PERMISSIONS);
        if (!(listed instanceof List<?> names))
        {
            return Set.of();
        }
        Set<Permission> needed = EnumSet.noneOf(Permission.class);
        for (Object name : names)
        {
            for (Permission each : Permission.values())
            {
                if (each.key().equals(name))
                {
                    needed.add(each);
                }
            }
        }
        return needed;
    }

    /**
     * An operation's record, expired first if it waited too long for its
     * approval.
     *
     * @param operation its record
     * @return the record now
     */
    private ControlStore.Operation expired(ControlStore.Operation operation)
    {
        if (operation.state() != ControlStore.State.AWAITING_APPROVAL
                || !(operation.parameters().get(APPROVE_BY)
                        instanceof String by))
        {
            return operation;
        }
        Instant until;
        try
        {
            until = Instant.parse(by);
        }
        catch (RuntimeException unreadable)
        {
            return operation;
        }
        if (Instant.now().isBefore(until))
        {
            return operation;
        }
        store.expire(operation.id());
        return store.find(operation.id(), scope).orElse(operation);
    }

    /**
     * A decision's body: a JSON object with a reason and nothing else.
     *
     * @param body the body
     * @return the reason, checked
     * @throws AdminArgs.UsageException if it isn't one
     */
    private static String reasonOnly(byte[] body)
    {
        Map<String, Object> fields;
        try
        {
            fields = body.length == 0 ? Map.of()
                    : JSONObjectUtils.parse(new String(body, UTF_8));
        }
        catch (ParseException | RuntimeException notJson)
        {
            throw new AdminArgs.UsageException("The body must be a JSON"
                    + " object");
        }
        for (String name : fields.keySet())
        {
            if (!name.equals(ControlRoute.REASON))
            {
                throw new AdminArgs.UsageException("Unknown field: " + name);
            }
        }
        Object reason = fields.get(ControlRoute.REASON);
        if (!(reason instanceof String text) || text.isBlank()
                || !REASON.matcher(text).matches())
        {
            throw new AdminArgs.UsageException("A decision needs a reason:"
                    + " 1 to 200 letters, digits, spaces or . , : _ # / -");
        }
        return text;
    }

    /**
     * One operation's record.
     *
     * @param id         its ID, as given
     * @param parameters the query's parameters: none
     * @param audit      the audit event
     * @param requestId  the request's ID
     * @return the answer
     */
    private Answer one(String id, Map<String, String[]> parameters,
            AuditRecord audit, String requestId)
    {
        if (!parameters.isEmpty())
        {
            throw new AdminArgs.UsageException("Unknown parameter: "
                    + parameters.keySet().iterator().next());
        }
        UUID operationId = uuid(id);
        audit.add(Attr.OPERATION_ID, operationId);
        Optional<ControlStore.Operation> found =
                store.find(operationId, scope);
        if (found.isEmpty())
        {
            return problem(404, "operation-not-found", "This stage has no"
                    + " such operation", requestId);
        }
        Refreshed refreshed = refresh(found.get());
        return new Answer(200, JSON, json(answer(refreshed.operation(),
                refreshed.unavailable())), null);
    }

    /**
     * A page of the stage's operation records.
     *
     * @param parameters the query's parameters: {@code limit},
     *                   {@code cursor}
     * @param requestId  the request's ID
     * @return the answer
     */
    private Answer page(Map<String, String[]> parameters, String requestId)
    {
        Map<String, Object> fields = queryFields(parameters);
        for (String name : fields.keySet())
        {
            if (!name.equals("limit") && !name.equals("cursor"))
            {
                throw new AdminArgs.UsageException("Unknown parameter: "
                        + name);
            }
        }
        int limit = DEFAULT_LIMIT;
        if (fields.containsKey("limit"))
        {
            try
            {
                limit = Integer.parseInt((String) fields.get("limit"));
            }
            catch (NumberFormatException wrong)
            {
                limit = -1;
            }
            if (limit < 1 || limit > MAX_LIMIT)
            {
                throw new AdminArgs.UsageException("limit must be from 1 to "
                        + MAX_LIMIT);
            }
        }
        ControlStore.Operation after = fields.containsKey("cursor")
                ? cursor((String) fields.get("cursor")) : null;
        List<ControlStore.Operation> listed = store.list(scope, limit + 1,
                after);
        boolean more = listed.size() > limit;
        List<ControlStore.Operation> shown = more
                ? listed.subList(0, limit) : listed;
        List<Object> operations = new ArrayList<>();
        for (ControlStore.Operation each : shown)
        {
            Refreshed refreshed = refresh(each);
            operations.add(answer(refreshed.operation(),
                    refreshed.unavailable()));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("operations", operations);
        body.put("next", more ? cursor(shown.getLast()) : null);
        return new Answer(200, JSON, json(body), null);
    }

    /**
     * An operation's record, and why it couldn't be brought up to date.
     *
     * @param operation   the record
     * @param unavailable the failure's class; null if it was, or needn't
     *                    be
     */
    private record Refreshed(ControlStore.Operation operation,
            String unavailable)
    {
    }

    /**
     * Brings a running operation the stage's workers carry out up to date
     * from the stage.
     *
     * @param recorded its record
     * @return the record now
     */
    @SuppressWarnings("unchecked")
    private Refreshed refresh(ControlStore.Operation recorded)
    {
        ControlStore.Operation operation = expired(recorded);
        if (operation.state() != ControlStore.State.RUNNING
                || operation.decidedBy() != null)
        {
            return new Refreshed(operation, null);
        }
        Optional<CliOperation<?, ?, ?>> found = cli.operation(
                operation.kind());
        if (found.isEmpty()
                || !(found.get() instanceof Followed<?, ?, ?> any))
        {
            return new Refreshed(operation, null);
        }
        Followed<Object, Object, Object> followed =
                (Followed<Object, Object, Object>) any;
        try (AutoCloseable target = open(operation.kind()))
        {
            Followed.Progress progress = followed.progress(
                    operation.parameters(), target);
            store.progress(operation.id(), switch (progress.state())
            {
                case RUNNING -> ControlStore.State.RUNNING;
                case DONE -> ControlStore.State.DONE;
                case FAILED -> ControlStore.State.FAILED;
            }, progress.details());
            return new Refreshed(store.find(operation.id(), scope)
                    .orElse(operation), null);
        }
        catch (Exception failure)
        {
            return new Refreshed(operation,
                    failure.getClass().getSimpleName());
        }
    }

    /**
     * An operation's record, as the API answers it.
     *
     * @param operation   the record
     * @param unavailable why it couldn't be brought up to date; null if
     *                    it was
     * @return its fields
     */
    static Map<String, Object> answer(ControlStore.Operation operation,
            String unavailable)
    {
        Map<String, Object> answer = Answers.object("id", operation.id(),
                "kind", operation.kind(), "scope", operation.scope(),
                "state", operation.state().wire(), "requestedBy",
                operation.requestedBy(), "reason", operation.reason(),
                "requestedAt", operation.requestedAt(), "updatedAt",
                operation.updatedAt(), "parameters", operation.parameters(),
                "progress", operation.progress());
        if (operation.ticket() != null)
        {
            answer.put("ticket", operation.ticket());
        }
        if (operation.decidedBy() != null)
        {
            answer.put("decidedBy", operation.decidedBy());
            answer.put("decision", operation.decision());
            answer.put("decidedAt", operation.decidedAt().toString());
        }
        if (!operation.result().isEmpty())
        {
            answer.put("result", operation.result());
        }
        if (unavailable != null)
        {
            answer.put("progressUnavailable", unavailable);
        }
        return answer;
    }

    /**
     * A cursor for the page after an operation.
     *
     * @param last the last operation of a page
     * @return the cursor, opaque to callers
     */
    private static String cursor(ControlStore.Operation last)
    {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                (last.requestedAt() + "|" + last.id()).getBytes(UTF_8));
    }

    /**
     * The operation a cursor follows.
     *
     * @param cursor the cursor
     * @return the operation, with its time and ID only
     * @throws AdminArgs.UsageException if it isn't a cursor
     */
    private static ControlStore.Operation cursor(String cursor)
    {
        try
        {
            String[] parts = new String(Base64.getUrlDecoder().decode(
                    cursor), UTF_8).split("\\|", 2);
            return new ControlStore.Operation(UUID.fromString(parts[1]),
                    null, null, Map.of(), null, null,
                    Instant.parse(parts[0]), ControlStore.State.RUNNING,
                    Map.of(), null, null, null, null, null, null, Map.of());
        }
        catch (RuntimeException wrong)
        {
            throw new AdminArgs.UsageException("Not a cursor");
        }
    }

    /**
     * What an action's body gives.
     *
     * @param fields its options and flags, by field
     * @param reason why, checked
     * @param input  what the command reads in place of standard input;
     *               for a sealed route, what follows the key
     * @param sealTo the fingerprint of the key to seal to; null if the
     *               route doesn't seal
     */
    private record Body(Map<String, Object> fields, String reason,
            byte[] input, String sealTo)
    {
    }

    /**
     * Reads an action's body: a JSON object with a {@code reason}, the
     * route's fields, and the record if the route takes one. Its reason
     * is audited.
     *
     * @param call  the request
     * @param audit the audit event
     * @return what it gives
     * @throws AdminArgs.UsageException if the key, the reason or the body
     *         isn't as the route needs
     */
    private static Body body(Call call, AuditRecord audit)
    {
        if (call.route().acts() && (call.key() == null
                || !KEY.matcher(call.key()).matches()))
        {
            throw new AdminArgs.UsageException("An action needs an"
                    + " Idempotency-Key header: 8 to 128 letters, digits"
                    + " or . _ : -");
        }
        Map<String, Object> fields;
        try
        {
            fields = call.body().length == 0 ? new LinkedHashMap<>()
                    : new LinkedHashMap<>(JSONObjectUtils.parse(
                            new String(call.body(), UTF_8)));
        }
        catch (ParseException | RuntimeException notJson)
        {
            throw new AdminArgs.UsageException("The body must be a JSON"
                    + " object");
        }
        Object reason = fields.remove(ControlRoute.REASON);
        if (!(reason instanceof String text) || text.isBlank()
                || !REASON.matcher(text).matches())
        {
            throw new AdminArgs.UsageException("An action needs a reason:"
                    + " 1 to 200 letters, digits, spaces or . , : _ # / -");
        }
        audit.add(Attr.ACTION_REASON, text);
        byte[] input = new byte[0];
        String sealTo = null;
        switch (call.route().input())
        {
            case REASON -> input = text.getBytes(UTF_8);
            case RECORD ->
            {
                Object record = fields.remove(ControlRoute.RECORD);
                if (!(record instanceof String encoded))
                {
                    throw new AdminArgs.UsageException("The corrected"
                            + " record is required, base64, as record");
                }
                try
                {
                    input = Base64.getDecoder().decode(encoded);
                }
                catch (IllegalArgumentException notBase64)
                {
                    throw new AdminArgs.UsageException("record must be"
                            + " base64");
                }
            }
            case PUBLIC_KEY ->
            {
                Object key = fields.remove(ControlRoute.PUBLIC_KEY);
                if (!(key instanceof String pem)
                        || pem.length() > ExportSealer.MAX_KEY_BYTES)
                {
                    throw new AdminArgs.UsageException("The public key is"
                            + " required, PEM, as publicKey");
                }
                input = pem.getBytes(UTF_8);
            }
            case SEAL -> sealTo = sealTo(fields);
            case SEAL_LOOKUP ->
            {
                sealTo = sealTo(fields);
                input = identifiers(fields.remove(ControlRoute.IDENTIFIERS));
            }
            case NONE ->
            {
            }
        }
        return new Body(fields, text, input, sealTo);
    }

    /**
     * The fingerprint of the key a sealed route seals to.
     *
     * @param fields the body's fields; the fingerprint is taken out
     * @return it
     * @throws AdminArgs.UsageException if it isn't given, or isn't one
     */
    private static String sealTo(Map<String, Object> fields)
    {
        Object fingerprint = fields.remove(ControlRoute.SEAL_TO);
        if (!(fingerprint instanceof String text)
                || !OperatorKeys.FINGERPRINT.matcher(text).matches())
        {
            throw new AdminArgs.UsageException("A sealed request needs"
                    + " sealTo: the fingerprint of one of your registered"
                    + " keys, 64 lower-case hex digits");
        }
        return text;
    }

    /**
     * The patient lookup's identifiers as the command reads them after
     * the key: one a line. They are PHI, so a refusal never repeats one.
     *
     * @param value the field's value; null for none
     * @return the lines
     * @throws AdminArgs.UsageException if it isn't a list of identifiers
     */
    private static byte[] identifiers(Object value)
    {
        if (value == null)
        {
            return new byte[0];
        }
        if (!(value instanceof List<?> list) || list.size() > MAX_IDENTIFIERS)
        {
            throw new AdminArgs.UsageException("identifiers must be a list"
                    + " of at most " + MAX_IDENTIFIERS);
        }
        StringBuilder lines = new StringBuilder();
        for (Object each : list)
        {
            if (!(each instanceof String identifier) || identifier.isBlank()
                    || identifier.length() > MAX_IDENTIFIER
                    || IDENTIFIER_CONTROL.matcher(identifier).find())
            {
                throw new AdminArgs.UsageException("Each identifier must be"
                        + " text of at most " + MAX_IDENTIFIER
                        + " characters, system|value or a value, with no"
                        + " control characters");
            }
            lines.append(identifier).append('\n');
        }
        return lines.toString().getBytes(UTF_8);
    }

    /**
     * A sealed route's input: the caller's key, then what follows it.
     *
     * @param pem  the key
     * @param rest what follows
     * @return them, a line apart
     */
    private static byte[] join(byte[] pem, byte[] rest)
    {
        byte[] joined = new byte[pem.length + 1 + rest.length];
        System.arraycopy(pem, 0, joined, 0, pem.length);
        joined[pem.length] = '\n';
        System.arraycopy(rest, 0, joined, pem.length + 1, rest.length);
        return joined;
    }

    /**
     * A query's parameters as fields: each given once.
     *
     * @param parameters the parameters
     * @return the fields, as text
     * @throws AdminArgs.UsageException if one is given twice
     */
    private static Map<String, Object> queryFields(
            Map<String, String[]> parameters)
    {
        Map<String, Object> fields = new LinkedHashMap<>();
        parameters.forEach((name, values) ->
        {
            if (values.length != 1)
            {
                throw new AdminArgs.UsageException(name + " given twice");
            }
            fields.put(name, values[0]);
        });
        return fields;
    }

    /**
     * The request as the command's own arguments: each option the route
     * takes, from its path variable, query parameter or body field, and
     * each flag set. A field the route doesn't take is refused.
     *
     * @param route     the route
     * @param variables the path's variables
     * @param fields    the query's parameters or the body's fields: text,
     *                  numbers, lists of them, and booleans for flags
     * @return the arguments, parsed as the CLI parses them
     * @throws AdminArgs.UsageException if a field isn't the route's
     */
    static AdminArgs arguments(ControlRoute route,
            Map<String, String> variables, Map<String, Object> fields)
    {
        for (String name : fields.keySet())
        {
            if (variables.containsKey(name)
                    || !route.options().containsKey(name)
                            && !route.flags().containsKey(name))
            {
                throw new AdminArgs.UsageException("Unknown field: " + name);
            }
        }
        List<String> args = new ArrayList<>();
        args.add(route.operation());
        route.options().forEach((name, option) ->
        {
            String value = variables.containsKey(name) ? variables.get(name)
                    : text(name, fields.get(name));
            if (value != null)
            {
                if (value.startsWith("--"))
                {
                    throw new AdminArgs.UsageException("Not a value for "
                            + name);
                }
                args.add("--" + option);
                args.add(value);
            }
        });
        route.flags().forEach((name, flag) ->
        {
            Object value = fields.get(name);
            if (value == null || Boolean.FALSE.equals(value)
                    || "false".equals(value))
            {
                return;
            }
            if (!Boolean.TRUE.equals(value) && !"true".equals(value))
            {
                throw new AdminArgs.UsageException(name
                        + " must be true or false");
            }
            args.add("--" + flag);
        });
        route.fixed().forEach(flag -> args.add("--" + flag));
        Set<String> flags = new HashSet<>(route.flags().values());
        flags.addAll(route.fixed());
        return AdminArgs.parse(args, flags);
    }

    /**
     * A field's value as an option's text: a list is joined by commas, as
     * the CLI takes several.
     *
     * @param name  the field
     * @param value its value; null if not given
     * @return the text; null if not given
     * @throws AdminArgs.UsageException if it isn't text, a number, or a
     *         list of them
     */
    private static String text(String name, Object value)
    {
        if (value == null || value instanceof String)
        {
            return (String) value;
        }
        if (value instanceof Number number)
        {
            return number.toString();
        }
        if (value instanceof List<?> list && !list.isEmpty()
                && list.stream().allMatch(each -> each instanceof String
                        || each instanceof Number))
        {
            return list.stream().map(String::valueOf)
                    .collect(Collectors.joining(","));
        }
        throw new AdminArgs.UsageException(name + " must be text, a number"
                + " or a list of them");
    }

    /**
     * A UUID as given.
     *
     * @param text the text
     * @return the UUID
     * @throws AdminArgs.UsageException if it isn't one
     */
    private static UUID uuid(String text)
    {
        try
        {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException wrong)
        {
            throw new AdminArgs.UsageException("Not an operation ID");
        }
    }

    /**
     * A request's hash, which a retry with its key must match: its method,
     * path and body.
     *
     * @param call the request
     * @return the SHA-256
     */
    private static byte[] hash(Call call)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((call.route().method() + " " + call.path() + "\n")
                    .getBytes(UTF_8));
            digest.update(call.body());
            return digest.digest();
        }
        catch (NoSuchAlgorithmException missing)
        {
            throw new IllegalStateException(missing);
        }
    }

    /**
     * An operation's result as its answer.
     *
     * @param operation the operation
     * @param result    its result
     * @return the answer
     */
    @SuppressWarnings("unchecked")
    private static Answer answered(Operation<?, ?, ?> operation,
            Object result)
    {
        Map<String, Object> answer = operation instanceof Answering<?>
                ? ((Answering<Object>) operation).answer(result)
                : Map.of();
        return new Answer(200, JSON, json(answer), null);
    }

    /**
     * Keeps a refusal as a key's answer, if it can be kept.
     *
     * @param principal the caller
     * @param key       the key
     * @param refused   the answer
     */
    private void keep(String principal, String key, Answer refused)
    {
        try
        {
            store.answered(principal, key, refused.status(), refused.type(),
                    refused.body(), refused.location());
        }
        catch (RuntimeException unkept)
        {
            // The key stays claimed with no answer: a retry with it is
            // answered in-progress, and the caller uses another.
        }
    }

    /**
     * A refusal's code, for its audit event.
     *
     * @param refused the refusal
     * @return its title
     */
    private static String title(Answer refused)
    {
        try
        {
            return String.valueOf(JSONObjectUtils.parse(refused.body())
                    .get("title"));
        }
        catch (ParseException unreadable)
        {
            return "refused";
        }
    }

    /**
     * Audits a request that was refused.
     *
     * @param event    the event's name
     * @param audit    the event's fields
     * @param decision why
     */
    private static void refuse(String event, AuditRecord audit,
            String decision)
    {
        Telemetry.get().auditWarn(event, audit.fields()
                .with(Attr.DECISION, decision)
                .with(Attr.RESULT, "refused"));
    }

    /**
     * A refusal (RFC 9457).
     *
     * @param status    the HTTP status
     * @param code      what kind of refusal, such as {@code denied}
     * @param detail    what to do about it; never stored data
     * @param requestId the request's ID, which its audit event names
     * @return the answer
     */
    static Answer problem(int status, String code, String detail,
            String requestId)
    {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", "urn:sdh:control:" + code);
        body.put("title", code);
        body.put("status", status);
        if (detail != null)
        {
            body.put("detail", detail);
        }
        if (requestId != null)
        {
            body.put("requestId", requestId);
        }
        return new Answer(status, PROBLEM, json(body), null);
    }

    /**
     * Fields as JSON.
     *
     * @param fields the fields
     * @return the JSON
     */
    private static String json(Map<String, Object> fields)
    {
        return JSONObjectUtils.toJSONString(fields);
    }

    /**
     * Sends an answer.
     *
     * @param response  where it goes
     * @param answer    the answer
     * @param requestId the request's ID, as a header; null for none
     * @throws IOException if it can't be written
     */
    static void send(HttpServletResponse response, Answer answer,
            String requestId) throws IOException
    {
        byte[] bytes = answer.body().getBytes(UTF_8);
        response.setStatus(answer.status());
        response.setContentType(answer.type());
        response.setCharacterEncoding(UTF_8.name());
        response.setHeader("Cache-Control", "no-store");
        if (requestId != null)
        {
            response.setHeader("X-Request-Id", requestId);
        }
        if (answer.location() != null)
        {
            response.setHeader("Location", answer.location());
        }
        response.setContentLength(bytes.length);
        response.getOutputStream().write(bytes);
    }

    /**
     * Rows streamed as JSON lines. The answer starts with the first row, so
     * an operation that fails before it is refused as any other; one that
     * fails part way ends with a line saying it is incomplete, since the
     * status is already sent.
     *
     * <p>A sealed answer (CONTROL-API-DESIGN.md §7) starts with a header
     * line, then a line per sealed part, {@code {"part": n, "jwe": …}},
     * and ends with a trailer giving the count of parts and the SHA-256 of
     * their JWEs, in order, as ASCII and with nothing between them. A
     * stream without its trailer was cut off, and is to be discarded.
     */
    private static final class Streamed implements Invocation.Sink
    {

        /** Where the rows go. */
        private final HttpServletResponse response;

        /** The request's ID, as a header. */
        private final String requestId;

        /** The response's body, once started. */
        private OutputStream out;

        /** How many rows were written. */
        private long count;

        /** A sealed answer's header line; null if it isn't sealed. */
        private final Map<String, Object> header;

        /** The sealed parts' digest; null if the answer isn't sealed. */
        private final MessageDigest digest;

        /** How many sealed parts were written. */
        private int parts;

        /**
         * Creates the stream; nothing is sent yet.
         *
         * @param response  where the rows go
         * @param requestId the request's ID
         * @param header    a sealed answer's header; null if it isn't
         *                  sealed
         */
        Streamed(HttpServletResponse response, String requestId,
                Map<String, Object> header)
        {
            this.response = response;
            this.requestId = requestId;
            this.header = header;
            try
            {
                this.digest = header == null ? null
                        : MessageDigest.getInstance("SHA-256");
            }
            catch (NoSuchAlgorithmException missing)
            {
                throw new IllegalStateException(missing);
            }
        }

        /**
         * How many sealed parts were sent.
         *
         * @return the count
         */
        int parts()
        {
            return parts;
        }

        /**
         * Whether a row has been sent.
         *
         * @return true once the answer has started
         */
        boolean started()
        {
            return out != null;
        }

        @Override
        public void row(Map<String, Object> row)
        {
            line(row);
            count++;
        }

        @Override
        public void sealed(String jwe)
        {
            if (header == null)
            {
                throw new IllegalStateException("Only a sealed route's"
                        + " answer holds sealed parts");
            }
            begin();
            parts++;
            digest.update(jwe.getBytes(StandardCharsets.US_ASCII));
            Map<String, Object> part = new LinkedHashMap<>();
            part.put("part", parts);
            part.put("jwe", jwe);
            line(part);
        }

        /** Ends the stream with its count, or answers no rows. */
        void finish()
        {
            if (header != null)
            {
                begin();
                Map<String, Object> trailer = new LinkedHashMap<>();
                trailer.put("end", true);
                trailer.put("parts", parts);
                trailer.put("sha256", HexFormat.of().formatHex(
                        digest.digest()));
                trailer.put("requestId", requestId);
                line(trailer);
                return;
            }
            Map<String, Object> end = new LinkedHashMap<>();
            end.put("end", true);
            end.put("rows", count);
            end.put("requestId", requestId);
            line(end);
        }

        /**
         * Ends a stream that failed part way.
         *
         * @param failure why
         */
        void broken(Exception failure)
        {
            Map<String, Object> end = new LinkedHashMap<>();
            end.put("end", false);
            end.put("incomplete", true);
            end.put(header == null ? "rows" : "parts",
                    header == null ? count : parts);
            end.put("reason", failure.getClass().getSimpleName());
            end.put("requestId", requestId);
            try
            {
                line(end);
            }
            catch (UncheckedIOException gone)
            {
                // The caller has gone; the audit event says what happened.
            }
        }

        /** Sends a sealed answer's header, unless it has been sent. */
        private void begin()
        {
            if (out == null)
            {
                line(header);
            }
        }

        /**
         * Writes one line, starting the answer if need be.
         *
         * @param fields the line's fields
         */
        private void line(Map<String, Object> fields)
        {
            try
            {
                if (out == null)
                {
                    response.setStatus(200);
                    response.setContentType(NDJSON);
                    response.setCharacterEncoding(UTF_8.name());
                    response.setHeader("Cache-Control", "no-store");
                    response.setHeader("X-Request-Id", requestId);
                    out = response.getOutputStream();
                }
                out.write((JSONObjectUtils.toJSONString(fields) + "\n")
                        .getBytes(UTF_8));
            }
            catch (IOException failure)
            {
                throw new UncheckedIOException(failure);
            }
        }
    }
}
