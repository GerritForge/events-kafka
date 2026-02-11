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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.ClientType;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaProducerProvider;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaSession;
import com.gerritforge.gerrit.plugins.kafka.session.Log4JKafkaMessageLogger;
import com.google.common.util.concurrent.Futures;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
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
  KafkaSession objectUnderTest;
  @Mock Producer<String, String> kafkaProducer;
  @Mock KafkaProducerProvider producerProvider;
  @Mock KafkaProperties properties;
  @Mock KafkaEventsPublisherMetrics publisherMetrics;

  @Mock Log4JKafkaMessageLogger msgLog;
  @Captor ArgumentCaptor<Callback> callbackCaptor;

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

    objectUnderTest = new KafkaSession(producerProvider, properties, publisherMetrics, msgLog);
  }

  @Test
  public void shouldIncrementBrokerMetricCounterWhenMessagePublishedInSyncMode() {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFuture(recordMetadata));
    objectUnderTest.connect();
    objectUnderTest.publish(message);
    verify(publisherMetrics, only()).incrementBrokerPublishedMessage();
  }

  @Test
  public void shouldUpdateMessageLogFileWhenMessagePublishedInSyncMode() {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFuture(recordMetadata));
    objectUnderTest.connect();
    objectUnderTest.publish(message);
    verify(msgLog).log(topic, message);
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenMessagePublishingFailedInSyncMode() {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenReturn(Futures.immediateFailedFuture(new Exception()));
    objectUnderTest.connect();
    objectUnderTest.publish(message);
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenUnexpectedExceptionInSyncMode() {
    when(properties.isSendAsync()).thenReturn(false);
    when(kafkaProducer.send(any())).thenThrow(new RuntimeException("Unexpected runtime exception"));
    try {
      objectUnderTest.connect();
      objectUnderTest.publish(message);
    } catch (RuntimeException e) {
      // expected
    }
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldIncrementBrokerMetricCounterWhenMessagePublishedInAsyncMode() {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any())).thenReturn(Futures.immediateFuture(recordMetadata));

    objectUnderTest.connect();
    objectUnderTest.publish(message);

    verify(kafkaProducer).send(any(), callbackCaptor.capture());
    callbackCaptor.getValue().onCompletion(recordMetadata, null);
    verify(publisherMetrics, only()).incrementBrokerPublishedMessage();
  }

  @Test
  public void shouldUpdateMessageLogFileWhenMessagePublishedInAsyncMode() {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any())).thenReturn(Futures.immediateFuture(recordMetadata));

    objectUnderTest.connect();
    objectUnderTest.publish(message);

    verify(kafkaProducer).send(any(), callbackCaptor.capture());
    callbackCaptor.getValue().onCompletion(recordMetadata, null);
    verify(msgLog).log(topic, message);
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenMessagePublishingFailedInAsyncMode() {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any()))
        .thenReturn(Futures.immediateFailedFuture(new Exception()));

    objectUnderTest.connect();
    objectUnderTest.publish(message);

    verify(kafkaProducer).send(any(), callbackCaptor.capture());
    callbackCaptor.getValue().onCompletion(null, new Exception());
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldIncrementBrokerFailedMetricCounterWhenUnexpectedExceptionInAsyncMode() {
    when(properties.isSendAsync()).thenReturn(true);
    when(kafkaProducer.send(any(), any()))
        .thenThrow(new RuntimeException("Unexpected runtime exception"));
    try {
      objectUnderTest.connect();
      objectUnderTest.publish(message);
    } catch (RuntimeException e) {
      // expected
    }
    verify(publisherMetrics, only()).incrementBrokerFailedToPublishMessage();
  }

  @Test
  public void shouldNotConnectKafkaSessionWhenBoostrapServersAreNotSet() {
    when(properties.getProperty("bootstrap.servers")).thenReturn(null);
    objectUnderTest.connect();
    assertThat(objectUnderTest.isOpen()).isFalse();
  }
}
