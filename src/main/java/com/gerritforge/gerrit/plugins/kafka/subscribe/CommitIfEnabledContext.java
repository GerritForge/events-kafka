package com.gerritforge.gerrit.plugins.kafka.subscribe;

import com.gerritforge.gerrit.eventbroker.MessageContext;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class CommitIfEnabledContext implements MessageContext {

  private final boolean autoCommitEnabled;
  private final ConsumerRecord<byte[], byte[]> consumerRecord;
  private final Consumer<byte[], byte[]> consumer;
  private final AtomicBoolean committed = new AtomicBoolean(false);

  CommitIfEnabledContext(
      boolean autoCommitEnabled,
      ConsumerRecord<byte[], byte[]> consumerRecord,
      Consumer<byte[], byte[]> consumer) {
    this.autoCommitEnabled = autoCommitEnabled;
    this.consumerRecord = consumerRecord;
    this.consumer = consumer;
  }

  @Override
  public void ack() {
    if (autoCommitEnabled && committed.compareAndSet(false, true)) {
      TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
      consumer.commitSync(Map.of(tp, new OffsetAndMetadata(consumerRecord.offset() + 1)));
    }
  }
}
