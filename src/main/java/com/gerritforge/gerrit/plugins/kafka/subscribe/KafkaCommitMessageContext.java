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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.TopicPartition;

public class KafkaCommitMessageContext implements MessageContext {
  /** Shared coordinator that aggregates record acks and performs periodic commits. */
  private final KafkaCommitCoordinator coordinator;

  /** Record partition for this context. */
  private final TopicPartition partition;

  /** Kafka record offset represented by this context. */
  private final long recordOffset;

  /** Ensures this context contributes at most one ack, even if called multiple times. */
  private final AtomicBoolean acked = new AtomicBoolean(false);

  /** Creates a record-scoped context bound to coordinator, partition and next offset. */
  KafkaCommitMessageContext(
      KafkaCommitCoordinator coordinator, TopicPartition partition, long recordOffset) {
    this.coordinator = coordinator;
    this.partition = partition;
    this.recordOffset = recordOffset;
  }

  /** Registers this record as acknowledged; actual broker commit is deferred to coordinator. */
  @Override
  public void ack() {
    if (acked.compareAndSet(false, true)) {
      coordinator.ack(partition, recordOffset);
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
   *   <li><b>record offset</b>: Kafka offset of the consumed record
   *   <li><b>highest contiguous acked record offset</b>: largest record offset such that all
   *       previous record offsets are also acknowledged
   * </ul>
   *
   * <p>Example: ack order {@code 1, 3, 2}. After acking {@code 1}, the highest staged offset is
   * {@code 1}. After acking {@code 3}, it is still {@code 1} (gap at {@code 2}). After acking
   * {@code 2}, it advances to {@code 3}.
   */
  static final class PartitionState {
    /** Acked record offsets not yet included in the contiguous staged sequence. */
    private final Set<Long> pendingAcks = new HashSet<>();

    /** Highest contiguous record offset currently staged for commit for this partition. */
    private long highestContiguousStaged;

    /** Last record offset successfully committed for this partition. */
    private long lastCommitted;

    PartitionState(long recordOffset) {
      this.highestContiguousStaged = recordOffset - 1;
      this.lastCommitted = recordOffset - 1;
    }

    /** Registers an ack and advances staged contiguous offsets only when gaps are filled. */
    synchronized void ack(long recordOffset) {
      if (recordOffset <= highestContiguousStaged) {
        return;
      }
      pendingAcks.add(recordOffset);
      while (pendingAcks.remove(highestContiguousStaged + 1)) {
        highestContiguousStaged++;
      }
    }

    /** Returns committable record offset, or -1 when nothing new is committable. */
    synchronized long recordOffsetToCommit() {
      return highestContiguousStaged > lastCommitted ? highestContiguousStaged : -1L;
    }

    /** Updates local committed record offset after successful broker commit. */
    synchronized void markCommitted(long committedRecordOffset) {
      if (committedRecordOffset > lastCommitted) {
        lastCommitted = committedRecordOffset;
      }
    }

    @Override
    public synchronized String toString() {
      return "PartitionState{"
          + "pendingAcks="
          + pendingAcks
          + ", highestContiguousStaged="
          + highestContiguousStaged
          + ", lastCommitted="
          + lastCommitted
          + '}';
    }
  }
}
