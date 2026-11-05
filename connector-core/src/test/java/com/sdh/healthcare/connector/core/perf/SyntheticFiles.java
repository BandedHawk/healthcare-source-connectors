/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;

/**
 * Synthetic input for throughput tests (design §13, §15): C-CDA documents,
 * X12 837 claims and CSV records, written to a target size. Content is
 * pseudo-random words from a large vocabulary, numbers and codes, so it
 * compresses roughly like real clinical text rather than collapsing to
 * nothing. Seeded, so runs are repeatable.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SyntheticFiles
{

    /**
     * A generated file.
     *
     * @param path where it was written
     * @param bytes its size
     * @param sections the number of sections it holds
     */
    public record Generated(Path path, long bytes, long sections)
    {
    }

    /** The words text is made from. */
    private static final String[] VOCAB = vocabulary(4096, 42);

    /** Given names. */
    private static final String[] GIVEN = {"Maria", "James", "Wei", "Aisha",
        "Olga", "Kwame", "Lucia", "Tom", "Priya", "Sven", "Fatima", "Diego",
        "Mei", "Noah", "Zara", "Ivan"};

    /** Family names. */
    private static final String[] FAMILY = {"Garcia", "Smith", "Chen",
        "Okafor", "Novak", "Mensah", "Rossi", "Brown", "Patel", "Larsen",
        "Haddad", "Silva", "Wong", "Miller", "Khan", "Petrov"};

    /** Not instantiable. */
    private SyntheticFiles()
    {
    }

    /**
     * Pseudo-words of 2–11 letters: enough distinct words that the text
     * doesn't compress unrealistically well.
     *
     * @param count the number of words
     * @param seed the random seed
     * @return the words
     */
    private static String[] vocabulary(int count, long seed)
    {
        SplittableRandom random = new SplittableRandom(seed);
        String[] out = new String[count];
        for (int index = 0; index < count; index++)
        {
            int length = 2 + random.nextInt(10);
            StringBuilder word = new StringBuilder(length);
            for (int letter = 0; letter < length; letter++)
            {
                word.append((char) ('a' + random.nextInt(26)));
            }
            out[index] = word.toString();
        }
        return out;
    }

    /**
     * Appends random words separated by spaces.
     *
     * @param out where to append
     * @param random the random source
     * @param count the number of words
     * @throws IOException if appending fails
     */
    private static void words(Appendable out, SplittableRandom random,
            int count) throws IOException
    {
        for (int index = 0; index < count; index++)
        {
            if (index > 0)
            {
                out.append(' ');
            }
            out.append(VOCAB[random.nextInt(VOCAB.length)]);
        }
    }

    /**
     * Random decimal digits.
     *
     * @param random the random source
     * @param count the number of digits
     * @return the digits
     */
    private static String digits(SplittableRandom random, int count)
    {
        StringBuilder out = new StringBuilder(count);
        for (int index = 0; index < count; index++)
        {
            out.append((char) ('0' + random.nextInt(10)));
        }
        return out.toString();
    }

    /**
     * A random date from 1940 to 2024.
     *
     * @param random the random source
     * @return the date as {@code yyyyMMdd}
     */
    private static String date(SplittableRandom random)
    {
        return String.format("%04d%02d%02d", 1940 + random.nextInt(85),
                1 + random.nextInt(12), 1 + random.nextInt(28));
    }

    /**
     * Counts bytes as they are written (all output is ASCII, so chars =
     * bytes).
     */
    private static final class Counting extends Writer
    {

        /** The real writer. */
        private final Writer out;

        /** The characters written so far. */
        long count;

        /**
         * Wraps a writer.
         *
         * @param out the real writer
         */
        Counting(Writer out)
        {
            this.out = out;
        }

        /**
         * Writes and counts characters.
         *
         * @param chars the characters
         * @param offset the first to write
         * @param length how many to write
         * @throws IOException if writing fails
         */
        @Override
        public void write(char[] chars, int offset, int length)
                throws IOException
        {
            out.write(chars, offset, length);
            count += length;
        }

        /**
         * Writes and counts part of a string.
         *
         * @param text the string
         * @param offset the first character to write
         * @param length how many to write
         * @throws IOException if writing fails
         */
        @Override
        public void write(String text, int offset, int length)
                throws IOException
        {
            out.write(text, offset, length);
            count += length;
        }

        /**
         * Flushes the real writer.
         *
         * @throws IOException if flushing fails
         */
        @Override
        public void flush() throws IOException
        {
            out.flush();
        }

        /**
         * Closes the real writer.
         *
         * @throws IOException if closing fails
         */
        @Override
        public void close() throws IOException
        {
            out.close();
        }
    }

    /**
     * Opens a file for counted ASCII writing through a 1 MiB buffer.
     *
     * @param path the file
     * @return the writer
     * @throws IOException if the file can't be opened
     */
    private static Counting open(Path path) throws IOException
    {
        BufferedWriter writer =
                Files.newBufferedWriter(path, StandardCharsets.US_ASCII);
        return new Counting(new BufferedWriter(writer, 1 << 20));
    }

    // ---- C-CDA: one ClinicalDocument per section, 50–350 KB each, inside
    // a batch wrapper ----

    /**
     * Writes C-CDA documents inside a batch wrapper until the target size.
     *
     * @param path the file to write
     * @param target the size to reach
     * @param seed the random seed
     * @return the file written
     * @throws IOException if writing fails
     */
    public static Generated ccda(Path path, long target, long seed)
            throws IOException
    {
        SplittableRandom random = new SplittableRandom(seed);
        long documents = 0;
        try (Counting writer = open(path))
        {
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    + "<batch xmlns=\"urn:hl7-org:v3\">\n");
            while (writer.count < target)
            {
                clinicalDocument(writer, random,
                        50_000 + random.nextInt(300_000));
                documents++;
            }
            writer.write("</batch>\n");
            writer.flush();
            return new Generated(path, writer.count, documents);
        }
    }

    /**
     * One standalone C-CDA document of 50–350 KB, as a producer would send
     * it: the XML declaration and a single {@code ClinicalDocument}.
     *
     * @param seed the random seed
     * @return the document, in ASCII
     */
    public static byte[] ccdaDocument(long seed)
    {
        SplittableRandom random = new SplittableRandom(seed);
        StringWriter text = new StringWriter(360_000);
        try (Counting writer = new Counting(text))
        {
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            clinicalDocument(writer, random,
                    50_000 + random.nextInt(300_000));
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
        return text.toString().getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Writes one ClinicalDocument of about a given size.
     *
     * @param writer where to write
     * @param random the random source
     * @param size the size to reach before closing the body
     * @throws IOException if writing fails
     */
    private static void clinicalDocument(Counting writer,
            SplittableRandom random, int size) throws IOException
    {
        long start = writer.count;
        writer.write("<ClinicalDocument xmlns=\"urn:hl7-org:v3\""
                + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                + ">\n");
        writer.write("  <realmCode code=\"US\"/>\n"
                + "  <typeId root=\"2.16.840.1.113883.1.3\""
                + " extension=\"POCD_HD000040\"/>\n");
        writer.write("  <templateId root=\"2.16.840.1.113883.10.20.22.1.2\""
                + " extension=\"2015-08-01\"/>\n");
        writer.write("  <id root=\"2.16.840.1.113883.19.5\" extension=\""
                + digits(random, 12) + "\"/>\n");
        writer.write("  <code code=\"34133-9\""
                + " codeSystem=\"2.16.840.1.113883.6.1\""
                + " displayName=\"Summarization of Episode Note\"/>\n");
        writer.write("  <title>Continuity of Care Document</title>\n"
                + "  <effectiveTime value=\"" + date(random) + "120000\"/>\n");
        writer.write("  <recordTarget><patientRole>"
                + "<id root=\"2.16.840.1.113883.19.5.99999.2\" extension=\""
                + digits(random, 9) + "\"/>\n"
                + "    <addr use=\"HP\"><streetAddressLine>"
                + (1 + random.nextInt(9999)) + " ");
        words(writer, random, 2);
        writer.write(" St</streetAddressLine><city>");
        words(writer, random, 1);
        writer.write("</city><state>MA</state><postalCode>0"
                + digits(random, 4) + "</postalCode></addr>\n");
        writer.write("    <patient><name use=\"L\"><given>"
                + GIVEN[random.nextInt(GIVEN.length)] + "</given><family>"
                + FAMILY[random.nextInt(FAMILY.length)] + "</family></name>\n"
                + "      <administrativeGenderCode code=\""
                + (random.nextBoolean() ? "F" : "M")
                + "\" codeSystem=\"2.16.840.1.113883.5.1\"/>"
                + "<birthTime value=\""
                + date(random) + "\"/></patient>\n"
                + "  </patientRole></recordTarget>\n");
        writer.write("  <component><structuredBody>\n");
        while (writer.count - start < size)
        {
            writer.write("    <component><section>\n"
                    + "      <templateId root=\"2.16.840.1.113883.10.20.22.2."
                    + (1 + random.nextInt(30)) + "\"/>\n"
                    + "      <code code=\"" + digits(random, 5) + "-"
                    + random.nextInt(10)
                    + "\" codeSystem=\"2.16.840.1.113883.6.1\"/>\n"
                    + "      <title>");
            words(writer, random, 3);
            writer.write("</title>\n      <text><table><tbody>\n");
            int rows = 5 + random.nextInt(20);
            for (int index = 0; index < rows; index++)
            {
                writer.write("        <tr><td>");
                words(writer, random, 3 + random.nextInt(6));
                writer.write("</td><td>" + date(random) + "</td><td>");
                words(writer, random, 5 + random.nextInt(15));
                writer.write("</td></tr>\n");
            }
            writer.write("      </tbody></table></text>\n");
            int entries = 2 + random.nextInt(8);
            for (int index = 0; index < entries; index++)
            {
                writer.write("      <entry>"
                        + "<observation classCode=\"OBS\" moodCode=\"EVN\">"
                        + "<id root=\"" + digits(random, 8)
                        + "\"/><code code=\"" + digits(random, 4) + "-"
                        + random.nextInt(10)
                        + "\" codeSystem=\"2.16.840.1.113883.6.1\"/>"
                        + "<effectiveTime value=\"" + date(random)
                        + "\"/><value xsi:type=\"PQ\" value=\""
                        + random.nextInt(500) + "." + random.nextInt(10)
                        + "\" unit=\"mg/dL\"/></observation></entry>\n");
            }
            writer.write("    </section></component>\n");
        }
        writer.write("  </structuredBody></component>\n"
                + "</ClinicalDocument>\n");
    }

    // ---- X12 837P: one ST…SE claim per section, 1–6 KB each, 1000 per GS
    // group, one group per interchange ----

    /**
     * Writes X12 837P interchanges until the target size.
     *
     * @param path the file to write
     * @param target the size to reach
     * @param seed the random seed
     * @return the file written
     * @throws IOException if writing fails
     */
    public static Generated x12(Path path, long target, long seed)
            throws IOException
    {
        SplittableRandom random = new SplittableRandom(seed);
        long transactions = 0;
        int interchange = 0;
        try (Counting writer = open(path))
        {
            while (writer.count < target)
            {
                interchange++;
                String control = String.format("%09d", interchange);
                writer.write("ISA*00*          *00*          *ZZ*SUBMITTER01"
                        + "    *ZZ*PAYER0000001   *261028*1200*^*00501*"
                        + control + "*0*P*:~\n");
                writer.write("GS*HC*SUBMITTER01*PAYER0000001*20261028*1200*"
                        + interchange + "*X*005010X222A1~\n");
                int inGroup = 0;
                while (inGroup < 1000 && writer.count < target)
                {
                    inGroup++;
                    transactions++;
                    claim(writer, random, String.format("%04d", inGroup));
                }
                writer.write("GE*" + inGroup + "*" + interchange + "~\n");
                writer.write("IEA*1*" + control + "~\n");
            }
            writer.flush();
            return new Generated(path, writer.count, transactions);
        }
    }

    /**
     * Writes one ST…SE claim transaction.
     *
     * @param writer where to write
     * @param random the random source
     * @param control the transaction control number
     * @throws IOException if writing fails
     */
    private static void claim(Writer writer, SplittableRandom random,
            String control) throws IOException
    {
        int segments = 0;
        StringBuilder out = new StringBuilder(4096);
        out.append("ST*837*").append(control).append("*005010X222A1~\n");
        out.append("BHT*0019*00*").append(digits(random, 10))
                .append("*20261028*1200*CH~\n");
        out.append("NM1*41*2*")
                .append(FAMILY[random.nextInt(FAMILY.length)].toUpperCase())
                .append(" CLINIC*****46*")
                .append(digits(random, 9)).append("~\n");
        out.append("PER*IC*")
                .append(GIVEN[random.nextInt(GIVEN.length)].toUpperCase())
                .append("*TE*").append(digits(random, 10))
                .append("~\n");
        out.append("NM1*40*2*PAYER*****46*").append(digits(random, 5))
                .append("~\n");
        out.append("HL*1**20*1~\nNM1*85*2*");
        words(out, random, 2);
        out.append("*****XX*").append(digits(random, 10)).append("~\nN3*")
                .append(1 + random.nextInt(9999)).append(' ');
        words(out, random, 2);
        out.append("~\nN4*");
        words(out, random, 1);
        out.append("*MA*0").append(digits(random, 8)).append("~\nREF*EI*")
                .append(digits(random, 9)).append("~\n");
        out.append("HL*2*1*22*0~\nSBR*P*18*").append(digits(random, 8))
                .append("******CI~\n");
        out.append("NM1*IL*1*")
                .append(FAMILY[random.nextInt(FAMILY.length)].toUpperCase())
                .append('*')
                .append(GIVEN[random.nextInt(GIVEN.length)].toUpperCase())
                .append("****MI*").append(digits(random, 11))
                .append("~\n");
        out.append("DMG*D8*").append(date(random)).append('*')
                .append(random.nextBoolean() ? 'F' : 'M').append("~\n");
        segments += 14;
        int lines = 1 + random.nextInt(25);
        out.append("CLM*").append(digits(random, 12)).append('*')
                .append(random.nextInt(5000))
                .append(".00***11:B:1*Y*A*Y*Y~\n");
        out.append("HI*ABK:").append((char) ('A' + random.nextInt(26)))
                .append(digits(random, 4)).append("*ABF:")
                .append((char) ('A' + random.nextInt(26)))
                .append(digits(random, 4)).append("~\n");
        segments += 2;
        for (int line = 1; line <= lines; line++)
        {
            out.append("LX*").append(line).append("~\nSV1*HC:")
                    .append(digits(random, 5)).append('*')
                    .append(random.nextInt(900))
                    .append(".00*UN*1***1~\nDTP*472*D8*")
                    .append(date(random)).append("~\nNTE*ADD*");
            words(out, random, 4 + random.nextInt(12));
            out.append("~\n");
            segments += 4;
        }
        // SE itself.
        segments += 1;
        out.append("SE*").append(segments).append('*').append(control)
                .append("~\n");
        writer.write(out.toString());
    }

    // ---- CSV: one record per section, ~150–400 bytes, header row; 1 in 10
    // notes quoted with a newline ----

    /**
     * Writes CSV records under a header row until the target size.
     *
     * @param path the file to write
     * @param target the size to reach
     * @param seed the random seed
     * @return the file written
     * @throws IOException if writing fails
     */
    public static Generated csv(Path path, long target, long seed)
            throws IOException
    {
        SplittableRandom random = new SplittableRandom(seed);
        long records = 0;
        try (Counting writer = open(path))
        {
            writer.write("patient_id,mrn,family,given,birth_date,sex,street,"
                    + "city,state,zip,phone,encounter_id,admit,"
                    + "discharge,dx_codes,note\n");
            StringBuilder out = new StringBuilder(512);
            while (writer.count < target)
            {
                records++;
                out.setLength(0);
                out.append(records).append(',').append(digits(random, 9))
                        .append(',')
                        .append(FAMILY[random.nextInt(FAMILY.length)])
                        .append(',')
                        .append(GIVEN[random.nextInt(GIVEN.length)])
                        .append(',').append(date(random)).append(',')
                        .append(random.nextBoolean() ? 'F' : 'M').append(',')
                        .append(1 + random.nextInt(9999)).append(' ');
                words(out, random, 2);
                out.append(" St,");
                words(out, random, 1);
                out.append(",MA,0").append(digits(random, 4)).append(',')
                        .append(digits(random, 10)).append(',')
                        .append(digits(random, 12)).append(',')
                        .append(date(random)).append(',')
                        .append(date(random)).append(",\"")
                        .append((char) ('A' + random.nextInt(26)))
                        .append(digits(random, 4)).append(';')
                        .append((char) ('A' + random.nextInt(26)))
                        .append(digits(random, 4)).append("\",");
                if (random.nextInt(10) == 0)
                {
                    out.append('"');
                    words(out, random, 5 + random.nextInt(20));
                    out.append("\n");
                    words(out, random, 3 + random.nextInt(10));
                    out.append('"');
                }
                else
                {
                    words(out, random, 5 + random.nextInt(30));
                }
                out.append('\n');
                writer.write(out.toString());
            }
            writer.flush();
            return new Generated(path, writer.count, records);
        }
    }
}
