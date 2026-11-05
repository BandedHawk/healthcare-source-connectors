/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.file.demo.OtlpReceiver.AuditEvent;
import com.sdh.healthcare.connector.file.demo.OtlpReceiver.SpanRecord;
import com.sdh.healthcare.connector.file.demo.PipelineDemo.Outcome;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The demo's telemetry, for review after the run (design §15.3): a
 * Markdown report that follows each item through the pipeline by what the
 * two apps exported. The landing node and the splitter each trace an item
 * on their own, with no shared trace, so the report joins them by the item
 * ID that both put on their audit events ({@code item_id} on the landing
 * node, {@code file_id} on the splitter).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class TelemetryReport
{

    /** Not instantiable. */
    private TelemetryReport()
    {
    }

    /**
     * Writes the report.
     *
     * @param otlp     the received telemetry
     * @param outcomes what became of each item
     * @param file     where to write it
     * @return the file
     * @throws IOException if it can't be written
     */
    static Path write(OtlpReceiver otlp, List<Outcome> outcomes, Path file)
            throws IOException
    {
        List<AuditEvent> events = otlp.events();
        List<SpanRecord> spans = otlp.spans();
        StringBuilder text = new StringBuilder();
        text.append("# Pipeline demo: telemetry report\n\n")
                .append("Written ").append(Instant.now()).append(". Each")
                .append(" item through the pipeline, from the audit events")
                .append(" and traces the landing node (`landing-connector`)")
                .append(" and the splitter (`file-connector`) exported over")
                .append(" OTLP. The two apps trace an item separately; the")
                .append(" item ID joins them. Times are from the item's")
                .append(" first event. The raw telemetry is beside this")
                .append(" file, as OTLP JSON: `<service>/traces.jsonl`,")
                .append(" `logs.jsonl` and `metrics.jsonl`.\n\n");

        text.append("## Items\n\n");
        text.append("| Pipeline | Item | Item ID | Landing | Splitter"
                + " | Sections | Unconverted | Seconds | As expected |\n");
        text.append("|---|---|---|---|---|---:|---:|---:|---|\n");
        for (Outcome outcome : outcomes)
        {
            boolean completed = outcome.status().equals("COMPLETED");
            text.append(String.format(Locale.ROOT,
                    "| %s | %s | %s | %s | %s | %s | %s | %.1f | %s |\n",
                    outcome.pipeline(), outcome.file().name(),
                    outcome.itemId() == null ? "–"
                            : "`" + outcome.itemId() + "`",
                    outcome.landing(), outcome.itemId() == null
                            ? "HTTP " + outcome.status() : outcome.status(),
                    completed ? outcome.sections() : "–",
                    completed ? outcome.failed() : "–",
                    outcome.millis() / 1000.0,
                    outcome.asExpected() ? "yes" : "**NO**"));
        }

        for (Outcome outcome : outcomes)
        {
            if (outcome.itemId() != null)
            {
                item(text, outcome, events, spans);
            }
        }

        text.append("\n## Refused pushes\n\n");
        text.append("The landing node's audit events for pushes it refused;"
                + " these never became items. A push from an unlisted client"
                + " is refused before it reaches the intake, so it has no"
                + " audit event: the HTTP 403 is all there is.\n\n");
        table(text, events.stream()
                .filter(event -> event.name().startsWith("push."))
                .toList());

        text.append("\n## The apps' own events\n\n");
        text.append("Starting, leases and the like: audit events about no"
                + " item.\n\n");
        table(text, events.stream()
                .filter(event -> event.get("item_id") == null
                        && event.get("file_id") == null
                        && !event.name().startsWith("push."))
                .toList());

        Files.createDirectories(file.getParent());
        Files.writeString(file, text.toString(), UTF_8);
        return file;
    }

    /**
     * Appends one item's section: its events from both apps in time order,
     * then each trace they were part of.
     *
     * @param text    where to write
     * @param outcome the item
     * @param events  every audit event
     * @param spans   every span
     */
    static void item(StringBuilder text, Outcome outcome,
            List<AuditEvent> events, List<SpanRecord> spans)
    {
        String id = outcome.itemId().toString();
        List<AuditEvent> own = events.stream()
                .filter(event -> id.equals(event.get("item_id"))
                        || id.equals(event.get("file_id")))
                .sorted(Comparator.comparingLong(AuditEvent::nanos))
                .toList();
        text.append("\n## ").append(outcome.pipeline()).append(": ")
                .append(outcome.file().name()).append("\n\n");
        text.append("Item `").append(id).append("`. Expected: ")
                .append(outcome.file().expected()).append(". Result: ")
                .append(outcome.landing()).append(", ")
                .append(outcome.status()).append(".\n\n");
        table(text, own);

        Set<String> traces = new LinkedHashSet<>();
        own.stream().map(AuditEvent::traceId)
                .filter(trace -> !trace.isEmpty()).forEach(traces::add);
        spans.stream().filter(span -> id.equals(span.attributes()
                .get("item_id")) || id.equals(span.attributes()
                        .get("file_id")))
                .map(SpanRecord::traceId).forEach(traces::add);
        for (String trace : traces)
        {
            List<SpanRecord> members = spans.stream()
                    .filter(span -> span.traceId().equals(trace)).toList();
            members.stream().filter(span -> span.parentId().isEmpty())
                    .findFirst().ifPresent(root -> text.append("\n```\n")
                            .append(OtlpReceiver.tree(members, root).strip())
                            .append("\n```\n"));
        }
    }

    /**
     * Appends a table of audit events, timed from the first.
     *
     * @param text   where to write
     * @param events the events, in order
     */
    static void table(StringBuilder text, List<AuditEvent> events)
    {
        if (events.isEmpty())
        {
            text.append("None.\n");
            return;
        }
        long first = events.getFirst().nanos();
        text.append("| ms | App | Severity | Event | Attributes |\n");
        text.append("|---:|---|---|---|---|\n");
        for (AuditEvent event : new ArrayList<>(events))
        {
            text.append(String.format(Locale.ROOT,
                    "| %,d | %s | %s | `%s` | %s |\n",
                    (event.nanos() - first) / 1_000_000, event.service(),
                    event.severity(), event.name(),
                    OtlpReceiver.text(event.attributes())
                            .replace("|", "\\|")));
        }
    }
}
