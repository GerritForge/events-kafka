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

import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.server.git.WorkQueue;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaCommitCoordinator {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  public interface Factory {
    KafkaCommitCoordinator create(Consumer<byte[], byte[]> consumer);
  }

  /** Upper bound for a single synchronous commit attempt. */
  private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(2);

  /** Kafka consumer used for the actual offset commit calls. */
  private final Consumer<byte[], byte[]> consumer;

  /** Comma-separated view of the subscribed topics, useful in debugger output. */
  private final String subscribedTopics;

  /** Per-partition commit state (acked offsets and last committed offset). */
  private final Map<TopicPartition, KafkaCommitMessageContext.PartitionState> states =
      new ConcurrentHashMap<>();

  /** Set by the scheduler and consumed by the receiver thread. */
  private final AtomicBoolean commitRequested = new AtomicBoolean(false);

  /** Periodic task that requests commit rounds at the configured interval. */
  private final ScheduledFuture<?> commitTask;

  @AssistedInject
  KafkaCommitCoordinator(
      WorkQueue workQueue,
      KafkaSubscriberProperties configuration,
      @Assisted Consumer<byte[], byte[]> consumer) {
    this.consumer = consumer;
    this.subscribedTopics = String.join(",", consumer.subscription());
    long commitIntervalMs = configuration.getCommitIntervalMs();
    logger.atWarning().log(
        "Created coordinator %s with commitIntervalMs=%s", this, commitIntervalMs);
    this.commitTask =
        workQueue
            .getDefaultQueue()
            .scheduleWithFixedDelay(
                () -> commitRequested.set(true),
                commitIntervalMs,
                commitIntervalMs,
                TimeUnit.MILLISECONDS);
  }

  /** Records an acknowledged record offset for later scheduled commit. */
  void ack(TopicPartition partition, long recordOffset) {
    KafkaCommitMessageContext.PartitionState state =
        states.computeIfAbsent(
            partition, ignored -> new KafkaCommitMessageContext.PartitionState(recordOffset));
    state.ack(recordOffset);
    logger.atWarning().log(
        "Coordinator %s acked partition %s at recordOffset=%s state=%s",
        this, partition, recordOffset, state);
  }

  void commitIfDue() {
    if (commitRequested.compareAndSet(true, false)) {
      commitNow();
    }
  }

  /** Forces one immediate commit round (used during shutdown/close). */
  void commitNow() {
    logger.atWarning().log("Coordinator %s commitNow with states=%s", this, states);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Map<TopicPartition, Long> committedRecordOffsets = new HashMap<>();
    states.forEach(
        (partition, state) -> {
          long recordOffset = state.recordOffsetToCommit();
          if (recordOffset >= 0) {
            offsets.put(partition, new OffsetAndMetadata(recordOffset + 1));
            committedRecordOffsets.put(partition, recordOffset);
          }
        });

    if (offsets.isEmpty()) {
      logger.atWarning().log("Coordinator %s has no offsets to commit", this);
      return;
    }

    logger.atWarning().log("Coordinator %s committing offsets=%s", this, offsets);
    consumer.commitSync(offsets, COMMIT_TIMEOUT);
    logger.atWarning().log("Coordinator %s committed offsets=%s", this, offsets);
    committedRecordOffsets.forEach(
        (partition, recordOffset) -> states.get(partition).markCommitted(recordOffset));
  }

  /** Stops the periodic scheduler for this coordinator. */
  void stop() {
    logger.atWarning().log("Stopping coordinator %s", this);
    commitTask.cancel(true);
  }

  @Override
  public String toString() {
    return "KafkaCommitCoordinator{" + "subscribedTopics='" + subscribedTopics + "'" + '}';
  }
}
