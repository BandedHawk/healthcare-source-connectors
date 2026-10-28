package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CheckpointCodecTest {
    private final CheckpointCodec codec = new CheckpointCodec(new SectionIds(new byte[32]));

    private static Checkpoint sample() {
        return Checkpoint.start(UUID.randomUUID(), "123:abc", "x12", 123, Instant.parse("2026-10-28T12:00:00Z"))
                .committed(ResumePoint.at(4096, 0, 106), 41, 20);
    }

    @Test
    void roundTrips() {
        Checkpoint c = sample();
        assertThat(codec.decode(codec.encode(c))).isEqualTo(c);
    }

    @Test
    void anyChangedFieldFailsVerification() {
        String json = codec.encode(sample());
        assertThatThrownBy(() -> codec.decode(json.replace("\"nextSeq\":42", "\"nextSeq\":43")
                .replace("\"committedSeq\":41", "\"committedSeq\":42")))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThatThrownBy(() -> codec.decode(json.replace("4096:0,106", "4096:0,107")))
                .isInstanceOf(CheckpointTamperedException.class);
        assertThatThrownBy(() -> codec.decode("{not json")).isInstanceOf(CheckpointTamperedException.class);
    }

    @Test
    void otherKeyFailsVerification() {
        byte[] other = new byte[32];
        other[0] = 1;
        String json = codec.encode(sample());
        assertThatThrownBy(() -> new CheckpointCodec(new SectionIds(other)).decode(json))
                .isInstanceOf(CheckpointTamperedException.class);
    }

    @Test
    void sequenceInvariantsAreEnforced() {
        Checkpoint c = sample();
        assertThatThrownBy(() -> new Checkpoint(c.fileId(), c.fingerprint(), c.format(), c.fileSize(), c.state(),
                c.resumeAt(), 10, 9, 10, c.startedAt(), null)).isInstanceOf(IllegalArgumentException.class);
    }
}
