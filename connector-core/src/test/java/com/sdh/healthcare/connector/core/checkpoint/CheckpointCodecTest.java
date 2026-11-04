/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CheckpointCodec}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CheckpointCodecTest
{

    /** The codec under test, with an all-zero ID key. */
    private final CheckpointCodec codec =
            new CheckpointCodec(new SectionIds(new byte[32]));

    /**
     * A checkpoint after one batch.
     *
     * @return the checkpoint
     */
    private static Checkpoint sample()
    {
        return Checkpoint.start(UUID.randomUUID(), "123:abc", "x12", 123,
                Instant.parse("2026-10-28T12:00:00Z"))
                .committed(ResumePoint.at(4096, 0, 106), 41, 20);
    }

    /** A checkpoint reads back. */
    @Test
    void roundTrips()
    {
        Checkpoint checkpoint = sample();
        assertThat(codec.decode(codec.encode(checkpoint)))
                .isEqualTo(checkpoint);
    }

    /**
     * The restart count is stored only once it is above 0, so a checkpoint
     * written before the count existed still verifies, and is MAC'd like
     * any other field once it is there.
     */
    @Test
    void restartCountIsStoredOnlyWhenSetAndIsProtected()
    {
        Checkpoint checkpoint = sample();
        String fresh = codec.encode(checkpoint);
        assertThat(fresh).doesNotContain("restarts");
        assertThat(codec.decode(fresh).restarts()).isZero();
        Checkpoint twice = checkpoint.resumed().resumed();
        String json = codec.encode(twice);
        assertThat(codec.decode(json)).isEqualTo(twice);
        assertThat(codec.decode(json).restarts()).isEqualTo(2);
        assertThatThrownBy(() -> codec.decode(
                json.replace("\"restarts\":2", "\"restarts\":0")))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThatThrownBy(() -> codec.decode(
                json.replace(",\"restarts\":2", "")))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThat(twice.committed(ResumePoint.at(8192, 0, 106), 50, 41)
                .restarts()).isZero();
    }

    /** Changing any field, or the JSON itself, fails verification. */
    @Test
    void anyChangedFieldFailsVerification()
    {
        String json = codec.encode(sample());
        assertThatThrownBy(() -> codec.decode(json
                .replace("\"nextSeq\":42", "\"nextSeq\":43")
                .replace("\"committedSeq\":41", "\"committedSeq\":42")))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThatThrownBy(() -> codec.decode(
                json.replace("4096:0,106", "4096:0,107")))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThatThrownBy(() -> codec.decode("{not json"))
                .isInstanceOf(CheckpointTamperedException.class);
    }

    /** A checkpoint MAC'd with another key fails verification. */
    @Test
    void otherKeyFailsVerification()
    {
        byte[] other = new byte[32];
        other[0] = 1;
        String json = codec.encode(sample());
        assertThatThrownBy(() -> new CheckpointCodec(new SectionIds(other))
                .decode(json))
                .isInstanceOf(CheckpointTamperedException.class);
    }

    /** Sequence numbers that don't fit together are refused. */
    @Test
    void sequenceInvariantsAreEnforced()
    {
        Checkpoint checkpoint = sample();
        assertThatThrownBy(() -> new Checkpoint(checkpoint.fileId(),
                checkpoint.fingerprint(), checkpoint.format(),
                checkpoint.fileSize(), checkpoint.state(),
                checkpoint.resumeAt(), 10, 9, 10, checkpoint.startedAt(),
                null, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
