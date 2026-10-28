package com.sdh.healthcare.connector.file.ingest;

import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointTamperedException;
import com.sdh.healthcare.connector.core.checkpoint.LeaseLostException;
import com.sdh.healthcare.connector.core.pipeline.FileProcessor;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.file.config.ConnectorProperties;
import com.sdh.healthcare.connector.file.runtime.FatalExit;
import com.sdh.healthcare.connector.file.source.LocalSourceFile;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.util.Optional;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles each file the inbound route picks up, one at a time (design §4.1, §5).
 *
 * <p>Before any file, a file left in progress (by a crash or an earlier error) is finished first: the inbound file
 * with its fingerprint is processed, or, if it has gone, the processor recovers or abandons it without the source.
 *
 * <p>Errors that mean the process can't safely continue (lease loss, unknown commit outcome, tampered checkpoint,
 * broken invariant) stop the process. Anything else (storage down, file unreadable) leaves the file where it is;
 * the route thread waits with backoff, so the pipeline pauses, and the next poll tries again. Paths and filenames
 * are never logged.
 */
public final class InboundService {
    private static final Logger LOG = LoggerFactory.getLogger(InboundService.class);
    private static final Duration MAX_BACKOFF = Duration.ofMinutes(5);

    private final FileProcessor processor;
    private final Path inbound;
    private final PathMatcher include;
    private final boolean markerMode;
    private final FatalExit fatal;
    private final Duration initialBackoff;
    private int consecutiveErrors;

    public InboundService(FileProcessor processor, ConnectorProperties.File props, Duration initialBackoff,
            FatalExit fatal) {
        this.processor = processor;
        this.inbound = props.inboundDir();
        this.include = FileSystems.getDefault().getPathMatcher("glob:" + props.include());
        this.markerMode = "marker".equals(props.readLock());
        this.initialBackoff = initialBackoff;
        this.fatal = fatal;
    }

    /** Called before the route starts. */
    public void recoverAtStartup() {
        try {
            settleInProgress();
        } catch (Throwable t) {
            onError(t);
        }
    }

    /** Called by the route for each settled file. */
    public void handle(Path file) {
        try {
            if (!Files.isRegularFile(file)) {
                return;
            }
            settleInProgress();
            if (!Files.isRegularFile(file)) {
                return; // it was the in-progress file, now finished
            }
            FileProcessor.Outcome o = processor.process(source(file));
            LOG.info("inbound.done fileId={} result={} sections={}", o.fileId(), o.result(), o.sectionCount());
            consecutiveErrors = 0;
        } catch (Throwable t) {
            onError(t);
        }
    }

    private void settleInProgress() throws Exception {
        Optional<Checkpoint> ip = processor.inProgress();
        if (ip.isEmpty()) {
            return;
        }
        Optional<Path> match = InProgressLocator.find(inbound, include, ip.get().fingerprint(), this::fingerprint);
        if (match.isPresent()) {
            LOG.info("inbound.resume fileId={} state={}", ip.get().fileId(), ip.get().state());
            processor.process(source(match.get()));
        } else {
            LOG.warn("inbound.in_progress_missing fileId={} state={}", ip.get().fileId(), ip.get().state());
            processor.recoverWithoutSource();
        }
    }

    private String fingerprint(Path p) throws IOException {
        try (SeekableByteSource src = SeekableByteSource.open(p)) {
            return processor.fingerprint(src);
        }
    }

    private LocalSourceFile source(Path file) {
        return markerMode
                ? new LocalSourceFile(file, file.resolveSibling(file.getFileName() + ".done"))
                : new LocalSourceFile(file);
    }

    private void onError(Throwable t) {
        if (isFatal(t)) {
            String reason = t instanceof LeaseLostException l ? "LEASE_LOST:" + l.reason()
                    : t instanceof CheckpointTamperedException ? "CHECKPOINT_TAMPERED"
                    : t instanceof CommitStateUnknownException ? "COMMIT_STATE_UNKNOWN"
                    : t.getClass().getSimpleName();
            LOG.error("inbound.fatal reason={}", reason);
            fatal.exit(reason);
            return;
        }
        consecutiveErrors++;
        long ms = Math.min(MAX_BACKOFF.toMillis(),
                initialBackoff.toMillis() << Math.min(consecutiveErrors - 1, 20));
        LOG.warn("inbound.retry error={} attempt={} backoff_ms={}", t.getClass().getSimpleName(), consecutiveErrors,
                ms);
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static boolean isFatal(Throwable t) {
        return t instanceof Error || t instanceof LeaseLostException || t instanceof CommitStateUnknownException
                || t instanceof CheckpointTamperedException || t instanceof IllegalStateException;
    }
}
