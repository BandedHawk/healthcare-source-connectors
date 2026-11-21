/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.Answering;
import com.sdh.healthcare.connector.stage.admin.Answers;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.ExportSealer;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.Given;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The control API's own operations on the principals' sealing keys
 * (CONTROL-API-DESIGN.md §7): list, register and revoke. They aren't the
 * CLI's: the CLI reads a key from standard input, its emergency path, and
 * has no registry. Each needs {@code keys.admin}, and a key can't be
 * registered by the principal it is for, so registering a key and sealing
 * to it are always two people.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class OperatorKeyCommands
{

    /** A principal's name: no control characters, at most 256. */
    private static final Pattern PRINCIPAL =
            Pattern.compile("[^\\p{Cntrl}]{1,256}");

    /** Not created: a holder of the operations. */
    private OperatorKeyCommands()
    {
    }

    /**
     * The operations, by name.
     *
     * @return {@code operator-keys}, {@code operator-key-register} and
     *         {@code operator-key-revoke}
     */
    public static List<CliOperation<OperatorKeys, ?, ?>> all()
    {
        return List.of(new ListKeys(), new RegisterKey(), new RevokeKey());
    }

    /**
     * A key, as the API answers it.
     *
     * @param key the key
     * @return its fields
     */
    static Map<String, Object> answer(OperatorKeys.Key key)
    {
        return Answers.object("fingerprint", key.fingerprint(), "principal",
                key.principal(), "registeredBy", key.registeredBy(),
                "registeredAt", key.registeredAt(), "expiresAt",
                key.expiresAt(), "revokedBy", key.revokedBy(), "revokedAt",
                key.revokedAt());
    }

    /**
     * A principal's name, as given.
     *
     * @param text the name
     * @return it
     * @throws AdminArgs.UsageException if it isn't one
     */
    static String principal(String text)
    {
        if (!PRINCIPAL.matcher(text).matches())
        {
            throw new AdminArgs.UsageException("principal must be 1 to 256"
                    + " characters, none a control character");
        }
        return text;
    }

    /**
     * A fingerprint, as given.
     *
     * @param text the fingerprint
     * @return it
     * @throws AdminArgs.UsageException if it isn't one
     */
    static String fingerprint(String text)
    {
        if (!OperatorKeys.FINGERPRINT.matcher(text).matches())
        {
            throw new AdminArgs.UsageException("A fingerprint is 64"
                    + " lower-case hex digits");
        }
        return text;
    }

    /** {@code operator-keys}: the keys registered, or one principal's. */
    static final class ListKeys implements
            CliOperation<OperatorKeys, String, List<OperatorKeys.Key>>,
            Answering<List<OperatorKeys.Key>>
    {

        /** Created by {@link #all()}. */
        ListKeys()
        {
        }

        @Override
        public String name()
        {
            return "operator-keys";
        }

        @Override
        public String usage()
        {
            return "GET /operator-keys[?principal=<name>]";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("principal");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.KEYS_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public String bind(AdminArgs arguments, OperatorKeys context)
        {
            return arguments.option("principal")
                    .map(OperatorKeyCommands::principal).orElse(null);
        }

        @Override
        public List<OperatorKeys.Key> run(String principal,
                OperatorKeys context, Invocation invocation)
        {
            invocation.audit().add(Attr.KEY_PRINCIPAL, principal);
            List<OperatorKeys.Key> keys = context.list(principal);
            invocation.audit().add(Attr.COUNT, keys.size());
            return keys;
        }

        @Override
        public Map<String, Object> answer(List<OperatorKeys.Key> keys)
        {
            return Answers.object("keys", Answers.list(keys,
                    OperatorKeyCommands::answer));
        }

        @Override
        public void print(List<OperatorKeys.Key> result, PrintStream out)
        {
            result.forEach(key -> out.println(key.fingerprint() + " "
                    + key.principal()));
        }
    }

    /** {@code operator-key-register}: registers a key for a principal. */
    static final class RegisterKey implements CliOperation<OperatorKeys,
            RegisterKey.Request, OperatorKeys.Key>,
            Answering<OperatorKeys.Key>
    {

        /**
         * What is asked.
         *
         * @param principal whose key it is
         * @param expiresAt when it expires
         * @param key       the key, PEM, read from the body
         */
        record Request(String principal, Instant expiresAt,
                Given<byte[]> key)
        {
        }

        /** Created by {@link #all()}. */
        RegisterKey()
        {
        }

        @Override
        public String name()
        {
            return "operator-key-register";
        }

        @Override
        public String usage()
        {
            return "POST /operator-keys {principal, publicKey, expiresAt,"
                    + " reason}";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("principal", "expires");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.KEYS_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public Request bind(AdminArgs arguments, OperatorKeys context)
        {
            String principal = principal(arguments.required("principal"));
            Instant expires;
            try
            {
                expires = Instant.parse(arguments.required("expires"));
            }
            catch (DateTimeParseException wrong)
            {
                throw new AdminArgs.UsageException("expiresAt must be an"
                        + " instant, such as 2027-01-31T00:00:00Z");
            }
            return new Request(principal, expires, () -> FailureCommands
                    .input(arguments.input(() -> new ByteArrayInputStream(
                            new byte[0])), ExportSealer.MAX_KEY_BYTES,
                            "public key"));
        }

        @Override
        public OperatorKeys.Key run(Request request, OperatorKeys context,
                Invocation invocation) throws Exception
        {
            AuditRecord audit = invocation.audit();
            audit.add(Attr.KEY_PRINCIPAL, request.principal());
            if (request.principal().equalsIgnoreCase(
                    invocation.principal()))
            {
                throw new Refusal(403, "not-for-yourself", "A principal's"
                        + " sealing key is registered by someone else");
            }
            byte[] pem = request.key().get();
            audit.add(Attr.SEALING_KEY, OperatorKeys.check(pem)
                    .fingerprint());
            return context.register(request.principal(), pem,
                    request.expiresAt(), invocation.principal());
        }

        @Override
        public Map<String, Object> answer(OperatorKeys.Key key)
        {
            return OperatorKeyCommands.answer(key);
        }

        @Override
        public void print(OperatorKeys.Key result, PrintStream out)
        {
            out.println(result.fingerprint());
        }
    }

    /** {@code operator-key-revoke}: revokes a key. */
    static final class RevokeKey implements
            CliOperation<OperatorKeys, String, OperatorKeys.Key>,
            Answering<OperatorKeys.Key>
    {

        /** Created by {@link #all()}. */
        RevokeKey()
        {
        }

        @Override
        public String name()
        {
            return "operator-key-revoke";
        }

        @Override
        public String usage()
        {
            return "DELETE /operator-keys/{fingerprint} {reason}";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("fingerprint");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.KEYS_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public String bind(AdminArgs arguments, OperatorKeys context)
        {
            return fingerprint(arguments.required("fingerprint"));
        }

        @Override
        public OperatorKeys.Key run(String fingerprint, OperatorKeys context,
                Invocation invocation)
        {
            invocation.audit().add(Attr.SEALING_KEY, fingerprint);
            OperatorKeys.Key key = context.find(fingerprint).orElseThrow(
                    () -> new Refusal(404, "key-not-found", "No key with"
                            + " this fingerprint is registered"));
            invocation.audit().add(Attr.KEY_PRINCIPAL, key.principal());
            if (key.revokedAt() == null)
            {
                context.revoke(fingerprint, invocation.principal());
            }
            return context.find(fingerprint).orElseThrow();
        }

        @Override
        public Map<String, Object> answer(OperatorKeys.Key key)
        {
            return OperatorKeyCommands.answer(key);
        }

        @Override
        public void print(OperatorKeys.Key result, PrintStream out)
        {
            out.println(result.fingerprint());
        }
    }
}
