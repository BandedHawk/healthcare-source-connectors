package com.sdh.healthcare.connector.file.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InProgressLocatorTest {
    @TempDir
    Path dir;

    @Test
    void findsTheFileWithTheMatchingFingerprintSkippingHiddenAndMarkers() throws Exception {
        Files.writeString(dir.resolve(".hidden"), "abc");
        Files.writeString(dir.resolve("x.done"), "abc");
        Path a = Files.writeString(dir.resolve("a.csv"), "abc");
        Path b = Files.writeString(dir.resolve("b.csv"), "xyz");
        Files.setLastModifiedTime(b, FileTime.from(Instant.now().minusSeconds(60)));
        Files.createDirectories(dir.resolve("completed"));
        var any = FileSystems.getDefault().getPathMatcher("glob:*");
        InProgressLocator.Fingerprinter fp = p -> "3:" + Files.readString(p);

        assertThat(InProgressLocator.find(dir, any, "3:abc", fp)).contains(a);
        assertThat(InProgressLocator.find(dir, any, "3:xyz", fp)).contains(b);
        assertThat(InProgressLocator.find(dir, any, "3:zzz", fp)).isEmpty();
        assertThat(InProgressLocator.find(dir, FileSystems.getDefault().getPathMatcher("glob:*.xml"), "3:abc", fp))
                .isEmpty();
        // Size mismatch is rejected without computing the fingerprint.
        assertThat(InProgressLocator.find(dir, any, "4:abc", p -> { throw new AssertionError("not called"); }))
                .isEmpty();
    }
}
