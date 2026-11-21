/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * {@code patients}: the patient lookup (HIPAA.md §4.2–§4.3, §5). Reads a
 * record stage's output, such as gold at the transform stage, and finds
 * the patients each FHIR record names by their identifiers. Two uses:
 * <ul>
 * <li><b>Who an incident touched</b> (45 CFR 164.410(c)(1)): given items,
 *     or a span of time, lists every record with its patients'
 *     identifiers.
 * <li><b>A patient's records</b> (access, amendment, accounting,
 *     §§ 164.524, 164.526, 164.528): given identifiers, lists the records
 *     that name them.
 * </ul>
 * The operator's public key, and any identifiers to look for, come on
 * standard input, never in the arguments, which may be logged. What is
 * found is sealed to that key, as {@code export} seals a failure: only the
 * key's holder can read it. The output names only counts in the clear, and
 * so does the audit. Every record read is audited ({@link
 * AdminCli#ADMIN_READER}). Reads only, but reads PHI: not a viewer's
 * command.
 *
 * @param <C> what the command reads from
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PatientsCommand<C extends PatientAdmin>
        implements AdminCommand<C>
{

    /** The FHIR JSON media type, whose records are read. */
    static final String FHIR_JSON = "application/fhir+json";

    /** How many bytes of found lines go in one sealed part, at most. */
    static final int PART_BYTES = 4 << 20;

    /** The longest standard input read: a key and identifiers. */
    static final int MAX_INPUT = 4 << 20;

    /** The longest span of time one run may cover. */
    static final Duration MAX_SPAN = Duration.ofDays(31);

    /** The end of a PEM public key, after which identifiers follow. */
    private static final String PEM_END = "-----END PUBLIC KEY-----";

    /** Creates the command. */
    public PatientsCommand()
    {
    }

    @Override
    public String name()
    {
        return "patients";
    }

    @Override
    public String usage()
    {
        return """
                patients (--item-id <id>[,<id>...] | --from <date|instant>
                          --to <date|instant>) < key-and-identifiers
                    Finds the patients the stage's FHIR records name, by
                    the identifiers of each Patient in them. Standard
                    input is the operator's public key (PEM, EC P-256 or
                    RSA), then, to look for patients, one identifier a
                    line: system|value, or a value for any system. With
                    none, lists every record of the items or span with its
                    patients' identifiers (who an incident touched); with
                    some, lists only the records naming them (a patient's
                    records). Every copy and revision is read; spans are
                    at most 31 days. The result is sealed (JWE) to the
                    key, on lines starting "sdh-export: ", each a part of
                    JSON lines: item, seq, record, patients. Shows only
                    counts. Every record read is audited.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id", "from", "to");
    }

    @Override
    public Set<String> flags()
    {
        return Set.of();
    }

    @Override
    public void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        audit.add(Attr.STAGE, context.stage());
        byte[] input = FailureCommands.input(context.patientInput(),
                MAX_INPUT, "key and identifiers");
        String text = new String(input, StandardCharsets.US_ASCII);
        Arrays.fill(input, (byte) 0);
        int end = text.indexOf(PEM_END);
        if (end < 0)
        {
            throw new AdminArgs.UsageException("Give a PEM public key on"
                    + " standard input, then any identifiers");
        }
        end += PEM_END.length();
        ExportSealer sealer = new ExportSealer(text.substring(0, end)
                .getBytes(StandardCharsets.US_ASCII));
        audit.add(Attr.RECIPIENT_KEY, sealer.fingerprint());
        List<PatientIdentifiers.Identifier> wanted = new ArrayList<>();
        for (String line : text.substring(end).split("\\R"))
        {
            String trimmed = line.strip();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#"))
            {
                wanted.add(PatientIdentifiers.Identifier.parse(trimmed));
            }
        }
        audit.add(Attr.OUTCOME, wanted.isEmpty() ? "list" : "find");

        Found found = new Found(sealer, wanted, out);
        if (arguments.option("item-id").isPresent())
        {
            if (arguments.option("from").isPresent()
                    || arguments.option("to").isPresent())
            {
                throw new AdminArgs.UsageException("Give --item-id or"
                        + " --from and --to, not both");
            }
            List<UUID> items = new ArrayList<>();
            for (String id : arguments.required("item-id").split(","))
            {
                items.add(FailureCommands.itemId(id.strip()));
            }
            audit.add(Attr.ITEM_ID, items.size() == 1 ? items.getFirst()
                    : null);
            context.records(items, found::visit);
        }
        else
        {
            Instant from = AuditCommands.time("from",
                    arguments.required("from"));
            Instant to = AuditCommands.time("to", arguments.required("to"));
            if (!to.isAfter(from)
                    || Duration.between(from, to).compareTo(MAX_SPAN) > 0)
            {
                throw new AdminArgs.UsageException("--to must be after"
                        + " --from, by at most 31 days");
            }
            context.records(from, to, found::visit);
        }
        found.finish();
        audit.add(Attr.COUNT, found.read);
        out.println("records read " + found.read + ", not FHIR "
                + found.skipped + ", with patients " + found.withPatients
                + ", listed " + found.listed + ", in " + found.parts
                + " sealed parts");
    }

    /** Collects what is found, and seals it in parts. */
    private static final class Found
    {

        /** Seals the parts. */
        private final ExportSealer sealer;

        /** The identifiers looked for; empty to list every record. */
        private final List<PatientIdentifiers.Identifier> wanted;

        /** Where sealed parts go. */
        private final PrintStream out;

        /** The current part's lines. */
        private final ByteArrayOutputStream part =
                new ByteArrayOutputStream();

        /** Records read. */
        private long read;

        /** Records not FHIR, not read. */
        private long skipped;

        /** Records naming a patient. */
        private long withPatients;

        /** Records listed. */
        private long listed;

        /** Parts sealed. */
        private int parts;

        /**
         * Starts collecting.
         *
         * @param sealer seals the parts
         * @param wanted the identifiers to look for
         * @param out    where sealed parts go
         */
        Found(ExportSealer sealer, List<PatientIdentifiers.Identifier> wanted,
                PrintStream out)
        {
            this.sealer = sealer;
            this.wanted = wanted;
            this.out = out;
        }

        /**
         * Takes a record: reads it if it is FHIR, and lists it if it names
         * a patient looked for, or, listing all, whatever it names.
         *
         * @param record  the record
         * @param content reads it
         * @throws Exception if it can't be read
         */
        void visit(PatientAdmin.StoredRecord record,
                PatientAdmin.ContentReader content) throws Exception
        {
            if (record.contentType() == null
                    || !record.contentType().startsWith(FHIR_JSON))
            {
                skipped++;
                return;
            }
            byte[] bytes = content.read();
            Set<PatientIdentifiers.Identifier> named;
            try
            {
                named = PatientIdentifiers.in(bytes);
            }
            finally
            {
                Arrays.fill(bytes, (byte) 0);
            }
            read++;
            if (!named.isEmpty())
            {
                withPatients++;
            }
            if (!wanted.isEmpty() && named.stream().noneMatch(found ->
                    wanted.stream().anyMatch(want -> want.matches(found))))
            {
                return;
            }
            List<Map<String, Object>> patients = new ArrayList<>();
            for (PatientIdentifiers.Identifier identifier : named)
            {
                Map<String, Object> each = new LinkedHashMap<>();
                each.put("system", identifier.system());
                each.put("value", identifier.value());
                patients.add(each);
            }
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("item", record.itemId().toString());
            line.put("seq", record.seq());
            line.put("record", record.recordId());
            line.put("patients", patients);
            part.writeBytes((JSONObjectUtils.toJSONString(line) + "\n")
                    .getBytes(StandardCharsets.UTF_8));
            listed++;
            if (part.size() >= PART_BYTES)
            {
                seal(false);
            }
        }

        /**
         * Seals what is left, as the last part; an empty last part says
         * nothing was found, or that nothing follows.
         */
        void finish()
        {
            seal(true);
        }

        /**
         * Seals the current part and prints it.
         *
         * @param last whether it is the last
         */
        private void seal(boolean last)
        {
            byte[] lines = part.toByteArray();
            part.reset();
            parts++;
            try
            {
                out.println(ExportSealer.LINE_PREFIX + sealer.seal(lines,
                        Map.of("sdh_part", parts, "sdh_last", last)));
            }
            finally
            {
                Arrays.fill(lines, (byte) 0);
            }
        }
    }
}
