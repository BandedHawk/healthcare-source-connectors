package com.sdh.healthcare.connector.core.perf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;

/**
 * Synthetic input for throughput tests (design §13, §15): C-CDA documents, X12 837 claims and CSV records, written
 * to a target size. Content is pseudo-random words from a large vocabulary, numbers and codes, so it compresses
 * roughly like real clinical text rather than collapsing to nothing. Seeded, so runs are repeatable.
 */
final class SyntheticFiles {
    record Generated(Path path, long bytes, long sections) {}

    private static final String[] VOCAB = vocabulary(4096, 42);
    private static final String[] GIVEN = {"Maria", "James", "Wei", "Aisha", "Olga", "Kwame", "Lucia", "Tom",
        "Priya", "Sven", "Fatima", "Diego", "Mei", "Noah", "Zara", "Ivan"};
    private static final String[] FAMILY = {"Garcia", "Smith", "Chen", "Okafor", "Novak", "Mensah", "Rossi",
        "Brown", "Patel", "Larsen", "Haddad", "Silva", "Wong", "Miller", "Khan", "Petrov"};

    private SyntheticFiles() {}

    /** Pseudo-words of 2–11 letters: enough distinct words that the text doesn't compress unrealistically well. */
    private static String[] vocabulary(int n, long seed) {
        SplittableRandom r = new SplittableRandom(seed);
        String[] out = new String[n];
        for (int i = 0; i < n; i++) {
            int len = 2 + r.nextInt(10);
            StringBuilder b = new StringBuilder(len);
            for (int j = 0; j < len; j++) {
                b.append((char) ('a' + r.nextInt(26)));
            }
            out[i] = b.toString();
        }
        return out;
    }

    private static void words(Appendable a, SplittableRandom r, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                a.append(' ');
            }
            a.append(VOCAB[r.nextInt(VOCAB.length)]);
        }
    }

    private static String digits(SplittableRandom r, int n) {
        StringBuilder b = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            b.append((char) ('0' + r.nextInt(10)));
        }
        return b.toString();
    }

    private static String date(SplittableRandom r) {
        return String.format("%04d%02d%02d", 1940 + r.nextInt(85), 1 + r.nextInt(12), 1 + r.nextInt(28));
    }

    /** Counts bytes as they are written (all output is ASCII, so chars = bytes). */
    private static final class Counting extends Writer {
        private final Writer out;
        long count;

        Counting(Writer out) {
            this.out = out;
        }

        @Override
        public void write(char[] c, int off, int len) throws IOException {
            out.write(c, off, len);
            count += len;
        }

        @Override
        public void write(String s, int off, int len) throws IOException {
            out.write(s, off, len);
            count += len;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static Counting open(Path p) throws IOException {
        BufferedWriter w = Files.newBufferedWriter(p, StandardCharsets.US_ASCII);
        return new Counting(new java.io.BufferedWriter(w, 1 << 20));
    }

    // ---- C-CDA: one ClinicalDocument per section, 50–350 KB each, inside a batch wrapper ----

    static Generated ccda(Path p, long target, long seed) throws IOException {
        SplittableRandom r = new SplittableRandom(seed);
        long docs = 0;
        try (Counting w = open(p)) {
            w.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<batch xmlns=\"urn:hl7-org:v3\">\n");
            while (w.count < target) {
                clinicalDocument(w, r, 50_000 + r.nextInt(300_000));
                docs++;
            }
            w.write("</batch>\n");
            w.flush();
            return new Generated(p, w.count, docs);
        }
    }

    private static void clinicalDocument(Counting w, SplittableRandom r, int size) throws IOException {
        long start = w.count;
        w.write("<ClinicalDocument xmlns=\"urn:hl7-org:v3\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");
        w.write("  <realmCode code=\"US\"/>\n  <typeId root=\"2.16.840.1.113883.1.3\" extension=\"POCD_HD000040\"/>\n");
        w.write("  <templateId root=\"2.16.840.1.113883.10.20.22.1.2\" extension=\"2015-08-01\"/>\n");
        w.write("  <id root=\"2.16.840.1.113883.19.5\" extension=\"" + digits(r, 12) + "\"/>\n");
        w.write("  <code code=\"34133-9\" codeSystem=\"2.16.840.1.113883.6.1\" displayName=\"Summarization of Episode Note\"/>\n");
        w.write("  <title>Continuity of Care Document</title>\n  <effectiveTime value=\"" + date(r) + "120000\"/>\n");
        w.write("  <recordTarget><patientRole><id root=\"2.16.840.1.113883.19.5.99999.2\" extension=\""
                + digits(r, 9) + "\"/>\n    <addr use=\"HP\"><streetAddressLine>" + (1 + r.nextInt(9999)) + " ");
        words(w, r, 2);
        w.write(" St</streetAddressLine><city>");
        words(w, r, 1);
        w.write("</city><state>MA</state><postalCode>0" + digits(r, 4) + "</postalCode></addr>\n");
        w.write("    <patient><name use=\"L\"><given>" + GIVEN[r.nextInt(GIVEN.length)] + "</given><family>"
                + FAMILY[r.nextInt(FAMILY.length)] + "</family></name>\n      <administrativeGenderCode code=\""
                + (r.nextBoolean() ? "F" : "M") + "\" codeSystem=\"2.16.840.1.113883.5.1\"/><birthTime value=\""
                + date(r) + "\"/></patient>\n  </patientRole></recordTarget>\n");
        w.write("  <component><structuredBody>\n");
        while (w.count - start < size) {
            w.write("    <component><section>\n      <templateId root=\"2.16.840.1.113883.10.20.22.2."
                    + (1 + r.nextInt(30)) + "\"/>\n      <code code=\"" + digits(r, 5) + "-" + r.nextInt(10)
                    + "\" codeSystem=\"2.16.840.1.113883.6.1\"/>\n      <title>");
            words(w, r, 3);
            w.write("</title>\n      <text><table><tbody>\n");
            int rows = 5 + r.nextInt(20);
            for (int i = 0; i < rows; i++) {
                w.write("        <tr><td>");
                words(w, r, 3 + r.nextInt(6));
                w.write("</td><td>" + date(r) + "</td><td>");
                words(w, r, 5 + r.nextInt(15));
                w.write("</td></tr>\n");
            }
            w.write("      </tbody></table></text>\n");
            int entries = 2 + r.nextInt(8);
            for (int i = 0; i < entries; i++) {
                w.write("      <entry><observation classCode=\"OBS\" moodCode=\"EVN\"><id root=\"" + digits(r, 8)
                        + "\"/><code code=\"" + digits(r, 4) + "-" + r.nextInt(10)
                        + "\" codeSystem=\"2.16.840.1.113883.6.1\"/><effectiveTime value=\"" + date(r)
                        + "\"/><value xsi:type=\"PQ\" value=\"" + r.nextInt(500) + "." + r.nextInt(10)
                        + "\" unit=\"mg/dL\"/></observation></entry>\n");
            }
            w.write("    </section></component>\n");
        }
        w.write("  </structuredBody></component>\n</ClinicalDocument>\n");
    }

    // ---- X12 837P: one ST…SE claim per section, 1–6 KB each, 1000 per GS group, one group per interchange ----

    static Generated x12(Path p, long target, long seed) throws IOException {
        SplittableRandom r = new SplittableRandom(seed);
        long transactions = 0;
        int interchange = 0;
        try (Counting w = open(p)) {
            while (w.count < target) {
                interchange++;
                String icn = String.format("%09d", interchange);
                w.write("ISA*00*          *00*          *ZZ*SUBMITTER01    *ZZ*PAYER0000001   *261028*1200*^*00501*"
                        + icn + "*0*P*:~\n");
                w.write("GS*HC*SUBMITTER01*PAYER0000001*20261028*1200*" + interchange + "*X*005010X222A1~\n");
                int inGroup = 0;
                while (inGroup < 1000 && w.count < target) {
                    inGroup++;
                    transactions++;
                    claim(w, r, String.format("%04d", inGroup));
                }
                w.write("GE*" + inGroup + "*" + interchange + "~\n");
                w.write("IEA*1*" + icn + "~\n");
            }
            w.flush();
            return new Generated(p, w.count, transactions);
        }
    }

    private static void claim(Writer w, SplittableRandom r, String control) throws IOException {
        int segments = 0;
        StringBuilder b = new StringBuilder(4096);
        b.append("ST*837*").append(control).append("*005010X222A1~\n");
        b.append("BHT*0019*00*").append(digits(r, 10)).append("*20261028*1200*CH~\n");
        b.append("NM1*41*2*").append(FAMILY[r.nextInt(FAMILY.length)].toUpperCase()).append(" CLINIC*****46*")
                .append(digits(r, 9)).append("~\n");
        b.append("PER*IC*").append(GIVEN[r.nextInt(GIVEN.length)].toUpperCase()).append("*TE*").append(digits(r, 10))
                .append("~\n");
        b.append("NM1*40*2*PAYER*****46*").append(digits(r, 5)).append("~\n");
        b.append("HL*1**20*1~\nNM1*85*2*");
        words(b, r, 2);
        b.append("*****XX*").append(digits(r, 10)).append("~\nN3*").append(1 + r.nextInt(9999)).append(' ');
        words(b, r, 2);
        b.append("~\nN4*");
        words(b, r, 1);
        b.append("*MA*0").append(digits(r, 8)).append("~\nREF*EI*").append(digits(r, 9)).append("~\n");
        b.append("HL*2*1*22*0~\nSBR*P*18*").append(digits(r, 8)).append("******CI~\n");
        b.append("NM1*IL*1*").append(FAMILY[r.nextInt(FAMILY.length)].toUpperCase()).append('*')
                .append(GIVEN[r.nextInt(GIVEN.length)].toUpperCase()).append("****MI*").append(digits(r, 11))
                .append("~\n");
        b.append("DMG*D8*").append(date(r)).append('*').append(r.nextBoolean() ? 'F' : 'M').append("~\n");
        segments += 14;
        int lines = 1 + r.nextInt(25);
        b.append("CLM*").append(digits(r, 12)).append('*').append(r.nextInt(5000)).append(".00***11:B:1*Y*A*Y*Y~\n");
        b.append("HI*ABK:").append((char) ('A' + r.nextInt(26))).append(digits(r, 4)).append("*ABF:")
                .append((char) ('A' + r.nextInt(26))).append(digits(r, 4)).append("~\n");
        segments += 2;
        for (int i = 1; i <= lines; i++) {
            b.append("LX*").append(i).append("~\nSV1*HC:").append(digits(r, 5)).append('*')
                    .append(r.nextInt(900)).append(".00*UN*1***1~\nDTP*472*D8*").append(date(r)).append("~\nNTE*ADD*");
            words(b, r, 4 + r.nextInt(12));
            b.append("~\n");
            segments += 4;
        }
        segments += 1; // SE itself
        b.append("SE*").append(segments).append('*').append(control).append("~\n");
        w.write(b.toString());
    }

    // ---- CSV: one record per section, ~150–400 bytes, header row; 1 in 10 notes quoted with a newline ----

    static Generated csv(Path p, long target, long seed) throws IOException {
        SplittableRandom r = new SplittableRandom(seed);
        long records = 0;
        try (Counting w = open(p)) {
            w.write("patient_id,mrn,family,given,birth_date,sex,street,city,state,zip,phone,encounter_id,admit,"
                    + "discharge,dx_codes,note\n");
            StringBuilder b = new StringBuilder(512);
            while (w.count < target) {
                records++;
                b.setLength(0);
                b.append(records).append(',').append(digits(r, 9)).append(',')
                        .append(FAMILY[r.nextInt(FAMILY.length)]).append(',').append(GIVEN[r.nextInt(GIVEN.length)])
                        .append(',').append(date(r)).append(',').append(r.nextBoolean() ? 'F' : 'M').append(',')
                        .append(1 + r.nextInt(9999)).append(' ');
                words(b, r, 2);
                b.append(" St,");
                words(b, r, 1);
                b.append(",MA,0").append(digits(r, 4)).append(',').append(digits(r, 10)).append(',')
                        .append(digits(r, 12)).append(',').append(date(r)).append(',').append(date(r)).append(",\"")
                        .append((char) ('A' + r.nextInt(26))).append(digits(r, 4)).append(';')
                        .append((char) ('A' + r.nextInt(26))).append(digits(r, 4)).append("\",");
                if (r.nextInt(10) == 0) {
                    b.append('"');
                    words(b, r, 5 + r.nextInt(20));
                    b.append("\n");
                    words(b, r, 3 + r.nextInt(10));
                    b.append('"');
                } else {
                    words(b, r, 5 + r.nextInt(30));
                }
                b.append('\n');
                w.write(b.toString());
            }
            w.flush();
            return new Generated(p, w.count, records);
        }
    }
}
