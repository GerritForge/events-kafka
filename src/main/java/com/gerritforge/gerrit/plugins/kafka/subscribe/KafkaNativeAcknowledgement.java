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

import com.gerritforge.gerrit.eventbroker.MessageAcknowledgement;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgementException;
import com.google.common.flogger.FluentLogger;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

class KafkaNativeAcknowledgement implements MessageAcknowledgement {
  private static final FluentLogger log = FluentLogger.forEnclosingClass();

  private final ConsumerRecord<byte[], byte[]> consumerRecord;
  private final Consumer<byte[], byte[]> consumer;
  private final AtomicBoolean acked = new AtomicBoolean(false);

  KafkaNativeAcknowledgement(
      ConsumerRecord<byte[], byte[]> consumerRecord, Consumer<byte[], byte[]> consumer) {
    this.consumerRecord = consumerRecord;
    this.consumer = consumer;
  }

  @Override
  public void ack() {
    if (acked.compareAndSet(false, true)) {
      TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
      long offset = consumerRecord.offset() + 1;
      try {
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)));
      } catch (KafkaException e) {
        throw new MessageAcknowledgementException(
            String.format("Failed to acknowledge offset %d on %s", offset, tp), e);
      }
      log.atWarning().log("Committed offset %d on %s", offset, tp);
    }
  }

  @Override
  public boolean isAutoAck() {
    return false;
  }
}
