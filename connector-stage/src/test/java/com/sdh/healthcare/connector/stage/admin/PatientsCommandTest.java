/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.crypto.ECDHDecrypter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PatientsCommand}: it finds the patients FHIR records
 * name, or the records naming given patients, and shows only counts, with
 * everything found sealed to the operator's key.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PatientsCommandTest
{

    /** A bundle naming one patient by two identifiers. */
    private static final String ALICE = """
            {"resourceType":"Bundle","entry":[
             {"resource":{"resourceType":"Patient","identifier":[
              {"system":"urn:oid:2.16.840.1.113883.19.5","value":"998991"},
              {"system":"http://hl7.org/fhir/sid/us-ssn","value":"111-00-2330"}
             ]}},
             {"resource":{"resourceType":"Condition","subject":
              {"reference":"Patient/1"}}}]}""";

    /** A resource with a contained patient. */
    private static final String BOB = """
            {"resourceType":"Encounter","contained":[
             {"resourceType":"Patient","identifier":[
              {"system":"urn:oid:2.16.840.1.113883.19.5","value":"12345"}]}]}
            """;

    /** The operator's key pair. */
    private KeyPair operator;

    /** The records the stage holds. */
    private final List<Stored> stored = new ArrayList<>();

    /** How many records were read. */
    private int reads;

    /** Creates the tests; JUnit runs each test on a new instance. */
    PatientsCommandTest()
    {
    }

    /**
     * A stored record and its content.
     *
     * @param record  the record
     * @param content its content
     */
    private record Stored(PatientAdmin.StoredRecord record, String content)
    {
    }

    /**
     * Makes the operator's key and the stage's records: two FHIR, one
     * not.
     *
     * @throws Exception if the key can't be made
     */
    @BeforeEach
    void setUp() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        operator = generator.generateKeyPair();
        UUID item = UUID.randomUUID();
        stored.add(new Stored(new PatientAdmin.StoredRecord(item, 0, "r0",
                PatientsCommand.FHIR_JSON), ALICE));
        stored.add(new Stored(new PatientAdmin.StoredRecord(item, 1, "r1",
                PatientsCommand.FHIR_JSON + ";fhirVersion=4.0"), BOB));
        stored.add(new Stored(new PatientAdmin.StoredRecord(item, 2, "r2",
                "application/json"), "{}"));
    }

    /**
     * Listing names every FHIR record with its patients' identifiers,
     * sealed; the clear output and the audit show counts only.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void listsEveryRecordsPatientsSealed() throws Exception
    {
        AuditRecord audit = new AuditRecord("alice");
        String out = run(audit, "", "--item-id",
                stored.getFirst().record().itemId().toString());

        assertThat(out).doesNotContain("998991").doesNotContain("12345")
                .contains("records read 2, not FHIR 1, with patients 2,"
                        + " listed 2, in 1 sealed parts");
        String found = unseal(out);
        assertThat(found).contains("\"record\":\"r0\"")
                .contains("{\"system\":\"urn:oid:2.16.840.1.113883.19.5\","
                        + "\"value\":\"998991\"}")
                .contains("\"record\":\"r1\"").contains("\"value\":\"12345\"")
                .doesNotContain("r2");
        assertThat(reads).isEqualTo(2);
        assertThat(audit.fields().toStrings())
                .containsEntry("sdh.outcome", "list")
                .containsEntry("sdh.count", "2")
                .containsKey("sdh.recipient_key")
                .doesNotContainValue("998991");
    }

    /**
     * Finding lists only the records naming an identifier given, by system
     * and value or by value alone.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void findsOnlyTheRecordsNamingAPatient() throws Exception
    {
        String bySystem = unseal(run(new AuditRecord("alice"),
                "urn:oid:2.16.840.1.113883.19.5|12345\n", "--from",
                "2026-11-01", "--to", "2026-11-02"));
        String byValue = unseal(run(new AuditRecord("alice"),
                "# comment\n111-00-2330\n", "--from", "2026-11-01", "--to",
                "2026-11-02"));
        String none = unseal(run(new AuditRecord("alice"),
                "other|12345\n", "--from", "2026-11-01", "--to",
                "2026-11-02"));

        assertThat(bySystem).contains("\"record\":\"r1\"")
                .doesNotContain("\"record\":\"r0\"");
        assertThat(byValue).contains("\"record\":\"r0\"")
                .doesNotContain("\"record\":\"r1\"");
        assertThat(none).isEmpty();
    }

    /**
     * Without a key, or with both selections, or too long a span, nothing
     * runs.
     */
    @Test
    void refusesWhatItCantSeal()
    {
        assertThatThrownBy(() -> run(new AuditRecord("alice"), null,
                "--item-id", UUID.randomUUID().toString()))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run(new AuditRecord("alice"), "",
                "--from", "2026-01-01", "--to", "2026-03-01"))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("31 days");
        assertThatThrownBy(() -> run(new AuditRecord("alice"), "",
                "--item-id", UUID.randomUUID().toString(), "--from",
                "2026-01-01", "--to", "2026-01-02"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThat(reads).isZero();
    }

    /**
     * Runs the command.
     *
     * @param audit       the audit event
     * @param identifiers what follows the key on standard input; null for
     *                    no key at all
     * @param args        its options
     * @return what it printed
     * @throws Exception if it fails
     */
    private String run(AuditRecord audit, String identifiers, String... args)
            throws Exception
    {
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(
                        operator.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
        byte[] input = (identifiers == null ? "nothing\n"
                : pem + identifiers).getBytes(UTF_8);
        List<String> all = new ArrayList<>(List.of("patients"));
        all.addAll(List.of(args));
        PatientsCommand<Context> command = new PatientsCommand<>();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        command.run(AdminArgs.parse(all, command.flags()),
                new Context(input), audit, new PrintStream(out, true, UTF_8));
        return out.toString(UTF_8);
    }

    /**
     * Decrypts the sealed parts with the operator's private key.
     *
     * @param out what the command printed
     * @return the parts' lines, in order
     * @throws Exception if a part can't be decrypted
     */
    private String unseal(String out) throws Exception
    {
        StringBuilder found = new StringBuilder();
        for (String line : out.split("\n"))
        {
            if (line.startsWith(ExportSealer.LINE_PREFIX))
            {
                JWEObject jwe = JWEObject.parse(line.substring(
                        ExportSealer.LINE_PREFIX.length()));
                jwe.decrypt(new ECDHDecrypter(
                        (ECPrivateKey) operator.getPrivate()));
                assertThat(jwe.getHeader().getCustomParam("sdh_part"))
                        .isNotNull();
                found.append(jwe.getPayload().toString());
            }
        }
        return found.toString();
    }

    /** The stage, holding {@link #stored}. */
    private final class Context implements PatientAdmin
    {

        /** Standard input. */
        private final byte[] input;

        /**
         * Creates the context.
         *
         * @param input standard input
         */
        Context(byte[] input)
        {
            this.input = input;
        }

        @Override
        public String stage()
        {
            return "transform";
        }

        @Override
        public InputStream patientInput()
        {
            return new ByteArrayInputStream(input);
        }

        @Override
        public void records(List<UUID> itemIds, Visitor visitor)
                throws Exception
        {
            Set<UUID> items = Set.copyOf(itemIds);
            for (Stored each : stored)
            {
                if (items.contains(each.record().itemId()))
                {
                    visit(each, visitor);
                }
            }
        }

        @Override
        public void records(Instant from, Instant to, Visitor visitor)
                throws Exception
        {
            for (Stored each : stored)
            {
                visit(each, visitor);
            }
        }

        /**
         * Visits one record, counting its reads.
         *
         * @param each    the record
         * @param visitor the visitor
         * @throws Exception if it fails
         */
        private void visit(Stored each, Visitor visitor) throws Exception
        {
            visitor.visit(each.record(), () ->
            {
                reads++;
                return each.content().getBytes(UTF_8);
            });
        }
    }
}
