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
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.Caller;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Operation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

/**
 * Serves the control API's operations (CONTROL-API-DESIGN.md §4, §10): each
 * request is bound into the operation's own request as the CLI binds it,
 * checked against the caller's permissions, run, answered, and audited as
 * {@code sdh.audit.control.<operation>} with the caller's authenticated
 * principal, whether it was allowed or not.
 *
 * <p>Nothing a caller didn't send is ever put in an answer or an event
 * besides the operation's own answer: a failure is answered and audited by
 * its class alone, never its message, which could quote stored data.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlService
{

    /** The authorities a caller's roles are given as, before the name. */
    public static final String ROLE = "role:";

    /** A JSON answer's media type. */
    static final String JSON = "application/json";

    /** A refusal's media type (RFC 9457). */
    static final String PROBLEM = "application/problem+json";

    /** Streamed rows' media type. */
    static final String NDJSON = "application/x-ndjson";

    /** The stage's operator commands. */
    private final AdminCli<Object, ?> cli;

    /** The stage's settings. */
    private final Object settings;

    /** The roles each permission is granted to. */
    private final Map<Permission, Set<String>> grants;

    /** How callers are authenticated, for the audit. */
    private final Caller.Auth auth;

    /**
     * Creates the service.
     *
     * @param cli      the stage's operator commands
     * @param settings the stage's settings
     * @param control  the control API's settings
     */
    public ControlService(AdminCli<Object, ?> cli, Object settings,
            ControlSettings control)
    {
        this.cli = cli;
        this.settings = settings;
        this.grants = control.grants();
        this.auth = control.auth().mode() == ControlSettings.Mode.JWT
                ? Caller.Auth.JWT : Caller.Auth.MTLS;
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
     * Serves a request.
     *
     * @param route      the route it came by
     * @param caller     the caller, authenticated
     * @param variables  the path's variables
     * @param parameters the query's parameters
     * @param response   where the answer goes
     * @throws IOException if the answer can't be written
     */
    @SuppressWarnings("unchecked")
    public void serve(ControlRoute route, Authentication caller,
            Map<String, String> variables, Map<String, String[]> parameters,
            HttpServletResponse response) throws IOException
    {
        String requestId = UUID.randomUUID().toString();
        Caller who = new Caller(caller.getName(), auth);
        AuditRecord audit = new AuditRecord(who.principal());
        audit.add(Attr.AUTH, auth.name().toLowerCase(Locale.ROOT));
        audit.add(Attr.REQUEST_ID, requestId);
        String event = "control." + route.operation().replace('-', '_');
        Optional<CliOperation<?, ?, ?>> found =
                cli.operation(route.operation());
        if (found.isEmpty())
        {
            refuse(event, audit, "not-on-this-stage");
            problem(response, 404, "not-on-this-stage", "This stage has no "
                    + route.operation() + " operation", requestId);
            return;
        }
        CliOperation<Object, Object, Object> operation =
                (CliOperation<Object, Object, Object>) found.get();
        Streamed rows = new Streamed(response, requestId);
        try (AutoCloseable target = cli.open(route.operation(), settings))
        {
            Object request;
            try
            {
                request = operation.bind(arguments(route, variables,
                        parameters), target);
            }
            catch (AdminArgs.UsageException failure)
            {
                refuse(event, audit, "bad-request");
                problem(response, 400, "bad-request", failure.getMessage(),
                        requestId);
                return;
            }
            Set<Permission> needed = operation.permissions(request);
            audit.add(Attr.PERMISSION, needed.stream().map(Permission::key)
                    .sorted().collect(Collectors.joining(",")));
            if (!granted(caller).containsAll(needed))
            {
                refuse(event, audit, "denied");
                problem(response, 403, "permission-required", "This needs "
                        + needed.stream().map(Permission::key).sorted()
                                .collect(Collectors.joining(" and ")),
                        requestId);
                return;
            }
            if (operation.approval(request) == Operation.Approval.FOUR_EYES)
            {
                refuse(event, audit, "needs-approval");
                problem(response, 403, "approval-required", "This needs a"
                        + " second person's approval", requestId);
                return;
            }
            audit.add(Attr.DECISION, "allowed");
            Object result = operation.run(request, target,
                    new Invocation(who, audit, rows));
            if (route.streamed())
            {
                rows.finish();
            }
            else
            {
                Map<String, Object> answer = operation instanceof Answering<?>
                        ? ((Answering<Object>) operation).answer(result)
                        : Map.of();
                write(response, 200, JSON, answer, requestId);
            }
            Telemetry.get().audit(event,
                    audit.fields().with(Attr.RESULT, "ok"));
        }
        catch (Exception failure)
        {
            Telemetry.get().auditWarn(event, audit.fields()
                    .with(Attr.RESULT, "failed")
                    .with(Attr.REASON, failure.getClass().getSimpleName()));
            if (rows.started())
            {
                rows.broken(failure);
            }
            else if (failure instanceof IllegalStateException)
            {
                problem(response, 409, "refused", failure.getClass()
                        .getSimpleName(), requestId);
            }
            else
            {
                problem(response, 500, "failed", failure.getClass()
                        .getSimpleName(), requestId);
            }
        }
    }

    /**
     * The request as the command's own arguments: each option the route
     * takes, from its path variable or query parameter, and each flag set.
     * A parameter the route doesn't take, or one given twice, is refused.
     *
     * @param route      the route
     * @param variables  the path's variables
     * @param parameters the query's parameters
     * @return the arguments, parsed as the CLI parses them
     * @throws AdminArgs.UsageException if a parameter isn't the route's
     */
    static AdminArgs arguments(ControlRoute route,
            Map<String, String> variables, Map<String, String[]> parameters)
    {
        List<String> args = new ArrayList<>();
        args.add(route.operation());
        for (Map.Entry<String, String[]> parameter : parameters.entrySet())
        {
            String name = parameter.getKey();
            if (!route.options().containsKey(name)
                    && !route.flags().containsKey(name))
            {
                throw new AdminArgs.UsageException("Unknown parameter: "
                        + name);
            }
            if (parameter.getValue().length != 1)
            {
                throw new AdminArgs.UsageException(name + " given twice");
            }
        }
        Map<String, String> given = new LinkedHashMap<>(variables);
        parameters.forEach((name, values) -> given.putIfAbsent(name,
                values[0]));
        route.options().forEach((name, option) ->
        {
            String value = given.get(name);
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
            String value = given.get(name);
            if (value == null || value.equals("false"))
            {
                return;
            }
            if (!value.equals("true"))
            {
                throw new AdminArgs.UsageException(name
                        + " must be true or false");
            }
            args.add("--" + flag);
        });
        route.fixed().forEach(flag -> args.add("--" + flag));
        return AdminArgs.parse(args, operationFlags(route));
    }

    /**
     * The flags the route may set, so the parser knows they take no value.
     *
     * @param route the route
     * @return them
     */
    private static Set<String> operationFlags(ControlRoute route)
    {
        Set<String> flags = new HashSet<>(route.flags().values());
        flags.addAll(route.fixed());
        return flags;
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
     * Answers with a refusal (RFC 9457).
     *
     * @param response  where it goes
     * @param status    the HTTP status
     * @param code      what kind of refusal, such as {@code denied}
     * @param detail    what to do about it; never stored data
     * @param requestId the request's ID, which its audit event names
     * @throws IOException if it can't be written
     */
    static void problem(HttpServletResponse response, int status,
            String code, String detail, String requestId) throws IOException
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
        write(response, status, PROBLEM, body, requestId);
    }

    /**
     * Writes a JSON answer.
     *
     * @param response  where it goes
     * @param status    the HTTP status
     * @param type      its media type
     * @param body      the answer
     * @param requestId the request's ID, as a header
     * @throws IOException if it can't be written
     */
    private static void write(HttpServletResponse response, int status,
            String type, Map<String, Object> body, String requestId)
            throws IOException
    {
        byte[] bytes = JSONObjectUtils.toJSONString(body).getBytes(UTF_8);
        response.setStatus(status);
        response.setContentType(type);
        response.setCharacterEncoding(UTF_8.name());
        response.setHeader("Cache-Control", "no-store");
        if (requestId != null)
        {
            response.setHeader("X-Request-Id", requestId);
        }
        response.setContentLength(bytes.length);
        response.getOutputStream().write(bytes);
    }

    /**
     * Rows streamed as JSON lines. The answer starts with the first row, so
     * an operation that fails before it is refused as any other; one that
     * fails part way ends with a line saying it is incomplete, since the
     * status is already sent.
     */
    private static final class Streamed implements Invocation.Sink
    {

        /** Where the rows go. */
        private final HttpServletResponse response;

        /** The response's body, once started. */
        private OutputStream out;

        /** The request's ID, as a header. */
        private final String requestId;

        /** How many rows were written. */
        private long count;

        /**
         * Creates the stream; nothing is sent yet.
         *
         * @param response  where the rows go
         * @param requestId the request's ID
         */
        Streamed(HttpServletResponse response, String requestId)
        {
            this.response = response;
            this.requestId = requestId;
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
            // Sealed results come with build step C5; no query streams
            // one.
            throw new UnsupportedOperationException("Sealed results aren't"
                    + " served yet");
        }

        /**
         * Ends the stream with its count, or answers no rows.
         */
        void finish()
        {
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
            end.put("rows", count);
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
