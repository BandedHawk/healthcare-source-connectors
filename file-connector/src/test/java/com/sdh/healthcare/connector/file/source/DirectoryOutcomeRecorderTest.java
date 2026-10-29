package com.sdh.healthcare.connector.file.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder;
import com.sdh.healthcare.connector.core.pipeline.OutcomeRecorder.ArchiveLocation;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DirectoryOutcomeRecorderTest {
    @TempDir
    Path inbound;

    @Test
    void writesPhiFreeRecordsWithOwnerOnlyPermissions() throws Exception {
        DirectoryOutcomeRecorder rec = new DirectoryOutcomeRecorder(inbound);
        UUID id = UUID.randomUUID();
        Instant t = Instant.parse("2026-10-28T12:00:00Z");
        rec.completed(new OutcomeRecorder.CompletionRecord(id, t, t.plusSeconds(5), 42, 2, 1000, "abc"));
        rec.completed(new OutcomeRecorder.CompletionRecord(id, t, t.plusSeconds(5), 42, 2, 1000, "abc")); // idempotent
        UUID id2 = UUID.randomUUID();
        rec.failed(new OutcomeRecorder.FailureRecord(id2, t, t.plusSeconds(1), "split",
                ErrorCode.PARSE_UNBALANCED_ELEMENT, 512, 3, 1000, ArchiveLocation.FAILED_FILES, "def"));

        Path c = inbound.resolve("completed").resolve(id + ".json");
        assertThat(Files.readString(c)).contains("\"sectionCount\":42", "\"failedSectionCount\":2", "\"fileDigest\":\"abc\"");
        assertThat(Files.readString(inbound.resolve("failed").resolve(id2 + ".json")))
                .contains("\"errorCode\":\"PARSE_UNBALANCED_ELEMENT\"", "\"archivedTo\":\"FAILED_FILES\"");
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(c))).isEqualTo("rw-------");
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(inbound.resolve("completed"))))
                .isEqualTo("rwx------");
        try (var s = Files.list(inbound.resolve("completed"))) {
            assertThat(s).hasSize(1); // no temp files left behind
        }
    }

    @Test
    void fallbackIsVisibleOnlyAfterCommit() throws Exception {
        DirectoryOutcomeRecorder rec = new DirectoryOutcomeRecorder(inbound);
        UUID id = UUID.randomUUID();
        Path target = inbound.resolve("failed").resolve(id + ".enc");
        try (OutcomeRecorder.FallbackFile f = rec.openFallback(id)) {
            f.out().write("partial".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(target).doesNotExist();
        try (var s = Files.list(inbound.resolve("failed"))) {
            assertThat(s).isEmpty();
        }
        try (OutcomeRecorder.FallbackFile f = rec.openFallback(id)) {
            f.out().write("whole".getBytes(StandardCharsets.UTF_8));
            f.commit();
        }
        assertThat(Files.readString(target)).isEqualTo("whole");
    }

    @Test
    void sourceFileDeletesIdempotently() throws Exception {
        Path p = Files.writeString(inbound.resolve("patient-smith.xml"), "<x/>");
        LocalSourceFile f = new LocalSourceFile(p);
        assertThat(f.originalName()).contains("patient-smith.xml");
        assertThat(f.toString()).doesNotContain("smith");
        try (var src = f.open()) {
            assertThat(src.size()).isEqualTo(4);
        }
        f.deleteOriginal();
        f.deleteOriginal();
        assertThat(p).doesNotExist();
    }
}
