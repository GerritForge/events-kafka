// Copyright (C) 2026 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.kafka.subscribe;

import com.gerritforge.gerrit.eventbroker.MessageContext;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Per-record {@link MessageContext} implementation.
 *
 * <p>Each instance represents one consumed record and maps {@link #ack()} to the subscriber-level
 * {@link CommitCoordinator}. The context is intentionally lightweight and stateful only for
 * idempotency of this specific record's ack.
 */
public class KafkaCommitMessageContext implements MessageContext {
  /** Shared coordinator that aggregates record acks and performs periodic commits. */
  private final CommitCoordinator coordinator;

  /** Record partition for this context. */
  private final TopicPartition partition;

  /** "Next offset" to commit for this record (record offset + 1). */
  private final long nextOffset;

  /** Ensures this context contributes at most one ack, even if called multiple times. */
  private final AtomicBoolean acked = new AtomicBoolean(false);

  /** Creates a record-scoped context bound to coordinator, partition and next offset. */
  KafkaCommitMessageContext(
      CommitCoordinator coordinator, TopicPartition partition, long nextOffset) {
    this.coordinator = coordinator;
    this.partition = partition;
    this.nextOffset = nextOffset;
  }

  /** Registers this record as acknowledged; actual broker commit is deferred to coordinator. */
  @Override
  public void ack() {
    if (acked.compareAndSet(false, true)) {
      coordinator.ack(partition, nextOffset);
    }
  }

  /**
   * Collects acks from any thread and performs commits only through {@link #ack(TopicPartition,
   * long)} and {@link #commitNow()}.
   *
   * <p>Commit safety rule: only <b>contiguous</b> acked offsets are committed per partition.
   *
   * <p>Contiguous means "no gaps". For a partition, if acked next-offsets are {@code [1, 2, 3]}, we
   * can safely commit {@code 3}. If acked next-offsets are {@code [1, 3]}, we must commit only
   * {@code 1} until {@code 2} is also acked.
   *
   * <p>This prevents committing past unacked records, which would otherwise risk skipping records
   * after restart/recovery.
   */
  static class CommitCoordinator {
    /** Upper bound for a single synchronous commit attempt. */
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(2);

    /** Kafka consumer used for the actual offset commit calls. */
    private final Consumer<byte[], byte[]> consumer;

    /** Minimum time between two commit attempts in milliseconds. */
    private final long commitIntervalMs;

    /** Per-partition commit state (acked offsets and last committed offset). */
    private final Map<TopicPartition, PartitionState> states = new ConcurrentHashMap<>();

    /** Last time (ms) we attempted a commit round. */
    private volatile long lastCommitMs;

    /** Initializes an interval-based coordinator for one subscriber/consumer instance. */
    CommitCoordinator(Consumer<byte[], byte[]> consumer, long commitIntervalMs) {
      this.consumer = consumer;
      this.commitIntervalMs = Math.max(1L, commitIntervalMs);
      this.lastCommitMs = System.currentTimeMillis();
    }

    /** Records an acknowledged next-offset and commits if interval elapsed. */
    void ack(TopicPartition partition, long nextOffset) {
      states.computeIfAbsent(partition, ignored -> new PartitionState()).ack(nextOffset);
      long now = System.currentTimeMillis();
      if (now - lastCommitMs >= commitIntervalMs) {
        commit(now);
      }
    }

    /** Forces one immediate commit round (used during shutdown/close). */
    void commitNow() {
      commit(System.currentTimeMillis());
    }

    /**
     * Builds commit payload from current contiguous acked offsets and calls commitSync.
     *
     * <p>If nothing is currently committable, this still advances {@code lastCommitMs} to avoid a
     * tight loop of empty commit attempts.
     */
    private void commit(long nowMs) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      states.forEach(
          (partition, state) -> {
            long nextOffset = state.nextOffsetToCommit();
            if (nextOffset > 0) {
              offsets.put(partition, new OffsetAndMetadata(nextOffset));
            }
          });

      if (offsets.isEmpty()) {
        lastCommitMs = nowMs;
        return;
      }

      consumer.commitSync(offsets, COMMIT_TIMEOUT);
      offsets.forEach((partition, offset) -> states.get(partition).markCommitted(offset.offset()));
      lastCommitMs = nowMs;
    }
  }

  /**
   * Mutable state for one partition.
   *
   * <p>Tracks acknowledged offsets, advances the highest contiguous offset staged for commit, and
   * remembers last committed offset to avoid redundant commits.
   *
   * <p>Terminology:
   *
   * <ul>
   *   <li><b>next-offset</b>: record offset + 1 (Kafka commit convention)
   *   <li><b>highest contiguous staged offset</b>: largest next-offset such that all previous
   *       next-offsets are also acknowledged
   * </ul>
   *
   * <p>Example: ack order {@code 1, 3, 2}. After acking {@code 1}, the highest staged offset is
   * {@code 1}. After acking {@code 3}, it is still {@code 1} (gap at {@code 2}). After acking
   * {@code 2}, it advances to {@code 3}.
   */
  private static final class PartitionState {
    /** Acked "next offsets" not yet included in the contiguous staged sequence. */
    private final Set<Long> pendingAcks = new HashSet<>();

    /** Highest contiguous next-offset currently staged for commit for this partition. */
    private long highestContiguousStaged = 0L;

    /** Last next-offset successfully committed for this partition. */
    private long lastCommitted = 0L;

    /** Registers an ack and advances staged contiguous offsets only when gaps are filled. */
    synchronized void ack(long nextOffset) {
      if (nextOffset <= highestContiguousStaged) {
        return;
      }
      pendingAcks.add(nextOffset);
      while (pendingAcks.remove(highestContiguousStaged + 1)) {
        highestContiguousStaged++;
      }
    }

    /** Returns committable next-offset, or -1 when nothing new is committable. */
    synchronized long nextOffsetToCommit() {
      return highestContiguousStaged > lastCommitted ? highestContiguousStaged : -1L;
    }

    /** Updates local committed offset after successful broker commit. */
    synchronized void markCommitted(long committedOffset) {
      if (committedOffset > lastCommitted) {
        lastCommitted = committedOffset;
      }
    }
  }
}
