package com.sdh.healthcare.connector.file.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.file.ingest.InboundDirectoryClaim.ClaimedException;
import com.sdh.healthcare.connector.file.ingest.InboundDirectoryClaim.Owner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** One connector per inbound directory (design §9.5). */
class InboundDirectoryClaimTest {
    static final Owner A = new Owner("file-connector", "file-connector", "sdh", "sdh_ingest");

    @TempDir
    Path dir;

    @Test
    void firstConnectorClaimsTheDirectoryAndKeepsItAcrossRestarts() throws Exception {
        InboundDirectoryClaim.claim(dir, A);
        Path file = dir.resolve(InboundDirectoryClaim.FILE);
        assertThat(file.getFileName().toString()).startsWith("."); // hidden: never picked up as input
        assertThat(Files.readString(file)).contains("lease-name=file-connector", "namespace=sdh_ingest")
                .doesNotContain(dir.toString());
        // A restart of the same connector is fine.
        InboundDirectoryClaim.claim(dir, A).verify();
    }

    @Test
    void anyDifferenceInIdentityIsAnotherConnector() throws Exception {
        InboundDirectoryClaim.claim(dir, A);
        for (Owner other : List.of(new Owner("file-connector", "other-lease", "sdh", "sdh_ingest"),
                new Owner("other", "file-connector", "sdh", "sdh_ingest"),
                new Owner("file-connector", "file-connector", "other", "sdh_ingest"),
                new Owner("file-connector", "file-connector", "sdh", "other_ns"))) {
            assertThatThrownBy(() -> InboundDirectoryClaim.claim(dir, other))
                    .isInstanceOfSatisfying(ClaimedException.class, e -> assertThat(e.holder()).isEqualTo(A))
                    .hasMessageContaining("lease-name=file-connector").hasMessageNotContaining(dir.toString());
        }
    }

    @Test
    void exactlyOneOfManyConnectorsStartingTogetherWins() throws Exception {
        int n = 16;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Owner o = new Owner("c" + i, "lease-" + i, "sdh", "sdh_ingest");
                Callable<Boolean> claim = () -> {
                    go.await();
                    try {
                        InboundDirectoryClaim.claim(dir, o);
                        return true;
                    } catch (ClaimedException e) {
                        return false;
                    }
                };
                results.add(pool.submit(claim));
            }
            go.countDown();
            int winners = 0;
            for (Future<Boolean> f : results) {
                winners += f.get() ? 1 : 0;
            }
            assertThat(winners).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aRunningConnectorStopsWhenItsClaimIsTakenAndRecreatesADeletedOne() throws Exception {
        InboundDirectoryClaim a = InboundDirectoryClaim.claim(dir, A);
        Path file = dir.resolve(InboundDirectoryClaim.FILE);

        Files.delete(file);
        a.verify();
        assertThat(file).exists();

        // Someone deletes it and another connector starts: the running one finds out before its next file.
        Files.delete(file);
        Owner b = new Owner("other", "other-lease", "sdh", "sdh_ingest");
        InboundDirectoryClaim.claim(dir, b);
        assertThatThrownBy(a::verify).isInstanceOfSatisfying(ClaimedException.class,
                e -> assertThat(e.holder()).isEqualTo(b));
    }

    @Test
    void anUnreadableClaimIsRefused() throws Exception {
        Files.writeString(dir.resolve(InboundDirectoryClaim.FILE), "not a claim\n");
        assertThatThrownBy(() -> InboundDirectoryClaim.claim(dir, A))
                .isInstanceOfSatisfying(ClaimedException.class, e -> assertThat(e.holder()).isNull())
                .hasMessageContaining("can't be read");
    }
}
