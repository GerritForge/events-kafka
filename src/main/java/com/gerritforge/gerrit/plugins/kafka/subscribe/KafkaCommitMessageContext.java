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
import com.google.common.flogger.FluentLogger;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

public class KafkaCommitMessageContext implements MessageContext {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final boolean autoCommitEnabled;
  private final ConsumerRecord<byte[], byte[]> consumerRecord;
  private final Consumer<byte[], byte[]> consumer;
  private final AtomicBoolean committed = new AtomicBoolean(false);

  KafkaCommitMessageContext(
      boolean autoCommitEnabled,
      ConsumerRecord<byte[], byte[]> consumerRecord,
      Consumer<byte[], byte[]> consumer) {
    this.autoCommitEnabled = autoCommitEnabled;
    this.consumerRecord = consumerRecord;
    this.consumer = consumer;
  }

  @Override
  public void ack() throws KafkaException {
    if (!autoCommitEnabled && committed.compareAndSet(false, true)) {
      TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
      try {
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(consumerRecord.offset() + 1)));
      } catch (KafkaException e) {
        logger.atSevere().withCause(e).log(
            "Commit failed for %s@%d (offset %d)",
            tp, consumerRecord.partition(), consumerRecord.offset());
        committed.set(false);
        throw e;
      }
    }
  }
}
