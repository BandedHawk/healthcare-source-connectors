package com.sdh.healthcare.connector.file.ingest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/** Finds the inbound file that matches the in-progress checkpoint's fingerprint (design §5, startup). */
final class InProgressLocator {

    @FunctionalInterface
    interface Fingerprinter {
        String of(Path file) throws IOException;
    }

    private InProgressLocator() {}

    /** Candidates are regular, non-hidden files directly in {@code dir}, matching {@code include}, oldest first. */
    static Optional<Path> find(Path dir, PathMatcher include, String fingerprint, Fingerprinter f) throws IOException {
        List<Path> candidates;
        try (Stream<Path> s = Files.list(dir)) {
            candidates = s.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .filter(p -> !p.getFileName().toString().endsWith(".done"))
                    .filter(p -> include.matches(p.getFileName()))
                    .sorted(Comparator.comparing(InProgressLocator::mtime))
                    .toList();
        }
        String size = fingerprint.substring(0, Math.max(0, fingerprint.indexOf(':')));
        for (Path p : candidates) {
            try {
                // Cheap size check first; the fingerprint starts with the file size.
                if (!size.isEmpty() && Files.size(p) != Long.parseLong(size)) {
                    continue;
                }
                if (f.of(p).equals(fingerprint)) {
                    return Optional.of(p);
                }
            } catch (IOException | NumberFormatException e) {
                // vanished or unreadable: not our file
            }
        }
        return Optional.empty();
    }

    private static long mtime(Path p) {
        try {
            return Files.getLastModifiedTime(p).toMillis();
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }
}
