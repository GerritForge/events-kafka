// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.kafka.publish;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.eventbroker.BrokerApiMessageListener;
import com.gerritforge.gerrit.eventbroker.log.MessageLogger;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.ClientType;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaProducerProvider;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaSession;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSessionTest {
  private static final int PARTITION = 1;
  private static final String PARTITION_ERROR = "Kafka partition %d does not exist for topic %s";
  private static final long TEST_TIMEOUT_SEC = 30;

  KafkaSession objectUnderTest;
  @Mock Producer<String, String> kafkaProducer;
  @Mock KafkaProducerProvider producerProvider;
  @Mock KafkaProperties properties;
  @Mock KafkaEventsPublisherMetrics publisherMetrics;
  @Mock BrokerApiMessageListener messageListener;

  @Captor ArgumentCaptor<Callback> callbackCaptor;
  @Captor ArgumentCaptor<ProducerRecord<String, String>> recordCaptor;

  RecordMetadata recordMetadata;
  String message = "sample_message";
  private String topic = "index";

  @Before
  public void setUp() {
    when(producerProvider.get()).thenReturn(kafkaProducer);
    when(properties.getTopic()).thenReturn(topic);
    when(properties.getProperty("bootstrap.servers")).thenReturn("localhost:9092");
    when(properties.getClientType()).thenReturn(ClientType.NATIVE);

    recordMetadata = new RecordMetadata(new TopicPartition(topic, 0), 0L, 0L, 0L, 0L, 0, 0);

    objectUnderTest = new KafkaSession(producerProvider, properties, publisherMetrics);
  }

  @Test
  public void shouldIncrementBrokerMetricCounterWhenMessagePublishedInSyncMode() throws Exception {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFuture(recordMetadata));
    objectUnderTest.connect();
    publish(message);
    verify(publisherMetrics, only()).incrementBrokerPublishedMessage();
  }

  @Test
  public void shouldNotifyMessageListenerWhenMessagePublishedInSyncMode() throws Exception {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFuture(recordMetadata));
    objectUnderTest.connect();
    objectUnderTest.setMessageListener(messageListener);
    publish(message);

    verify(messageListener, only())
        .messageProcessed(MessageLogger.Direction.PUBLISH, topic, message);
  }

  @Test
  public void shouldPublishSyncMessageToPartition() {
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFuture(recordMetadata));
    connectWithPartition();

    objectUnderTest.publish(topic, Optional.of(PARTITION), message);

    verify(kafkaProducer).send(recordCaptor.capture());
    assertThat(recordCaptor.getValue().partition()).isEqualTo(PARTITION);
  }

  @Test
  public void shouldPublishAsyncMessageToPartition() {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any())).thenReturn(Futures.immediateFuture(recordMetadata));
    connectWithPartition();

    objectUnderTest.publish(topic, Optional.of(PARTITION), message);

    verify(kafkaProducer).send(recordCaptor.capture(), any());
    assertThat(recordCaptor.getValue().partition()).isEqualTo(PARTITION);
  }

  @Test
  public void shouldFailWhenPartitionDoesNotExist() {
    when(kafkaProducer.partitionsFor(topic)).thenReturn(List.of());
    objectUnderTest.connect();

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> objectUnderTest.publish(topic, Optional.of(PARTITION), message));

    assertThat(thrown).hasMessageThat().isEqualTo(String.format(PARTITION_ERROR, PARTITION, topic));
  }

  @Test
  public void shouldValidatePartitionOnlyOnce() {
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFuture(recordMetadata));
    connectWithPartition();

    objectUnderTest.publish(topic, Optional.of(PARTITION), message);
    objectUnderTest.publish(topic, Optional.of(PARTITION), message);

    verify(kafkaProducer, times(1)).partitionsFor(topic);
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenMessagePublishingFailedInSyncMode() {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFailedFuture(new Exception()));
    objectUnderTest.connect();
    assertThrows(ExecutionException.class, () -> publish(message));
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenUnexpectedExceptionInSyncMode() {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenThrow(new RuntimeException("Unexpected runtime exception"));
    objectUnderTest.connect();
    assertThrows(ExecutionException.class, () -> publish(message));
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldIncrementBrokerMetricCounterWhenMessagePublishedInAsyncMode() throws Exception {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any())).thenReturn(Futures.immediateFuture(recordMetadata));

    objectUnderTest.connect();
    publish(message);

    verify(kafkaProducer).send(any(), callbackCaptor.capture());
    callbackCaptor.getValue().onCompletion(recordMetadata, null);
    verify(publisherMetrics, only()).incrementBrokerPublishedMessage();
  }

  @Test
  public void shouldNotifyMessageListenerWhenMessagePublishedInAsyncMode() throws Exception {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any())).thenReturn(Futures.immediateFuture(recordMetadata));

    objectUnderTest.connect();
    objectUnderTest.setMessageListener(messageListener);
    publish(message);

    verify(kafkaProducer).send(any(), callbackCaptor.capture());
    callbackCaptor.getValue().onCompletion(recordMetadata, null);

    verify(messageListener, only())
        .messageProcessed(MessageLogger.Direction.PUBLISH, topic, message);
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenMessagePublishingFailedInAsyncMode()
      throws Exception {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any()))
        .thenReturn(Futures.immediateFailedFuture(new Exception()));

    objectUnderTest.connect();
    assertThrows(ExecutionException.class, () -> publish(message));

    verify(kafkaProducer).send(any(), callbackCaptor.capture());
    callbackCaptor.getValue().onCompletion(null, new Exception());
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenUnexpectedExceptionInAsyncMode()
      throws Exception {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any()))
        .thenThrow(new RuntimeException("Unexpected runtime exception"));
    objectUnderTest.connect();
    assertThrows(ExecutionException.class, () -> publish(message));

    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldNotConnectKafkaSessionWhenBoostrapServersAreNotSet() {
    when(properties.getProperty("bootstrap.servers")).thenReturn(null);
    objectUnderTest.connect();
    assertThat(objectUnderTest.isOpen()).isFalse();
  }

  private void connectWithPartition() {
    PartitionInfo partitionInfo = mock(PartitionInfo.class);
    when(partitionInfo.partition()).thenReturn(PARTITION);
    when(kafkaProducer.partitionsFor(topic)).thenReturn(List.of(partitionInfo));
    objectUnderTest.connect();
  }

  private void publish(String messageBody) throws Exception {
    assertThat(
            objectUnderTest
                .publish(properties.getTopic(), Optional.empty(), messageBody)
                .get(TEST_TIMEOUT_SEC, TimeUnit.SECONDS))
        .isTrue();
  }
}
