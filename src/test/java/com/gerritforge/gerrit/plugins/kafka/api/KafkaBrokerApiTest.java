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

package com.gerritforge.gerrit.plugins.kafka.api;

import static com.gerritforge.gerrit.eventbroker.TopicSubscriber.topicSubscriber;
import static com.gerritforge.gerrit.eventbroker.TopicSubscriberWithGroupId.topicSubscriberWithGroupId;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.gerritforge.gerrit.eventbroker.ContextAwareConsumer;
import com.gerritforge.gerrit.eventbroker.MessageContext;
import com.gerritforge.gerrit.plugins.kafka.KafkaContainerProvider;
import com.gerritforge.gerrit.plugins.kafka.KafkaRestContainer;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.ClientType;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaProducerProvider;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaSession;
import com.gerritforge.gerrit.plugins.kafka.session.Log4JKafkaMessageLogger;
import com.google.common.base.Strings;
import com.google.gerrit.extensions.registration.DynamicMap;
import com.google.gerrit.metrics.MetricMaker;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.EventGson;
import com.google.gerrit.server.events.EventGsonProvider;
import com.google.gerrit.server.events.ProjectCreatedEvent;
import com.google.gerrit.server.git.WorkQueue;
import com.google.gerrit.server.plugincontext.PluginContext;
import com.google.gerrit.server.plugincontext.PluginMapContext;
import com.google.gerrit.server.util.IdGenerator;
import com.google.gerrit.server.util.OneOffRequestContext;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

@RunWith(MockitoJUnitRunner.class)
public class KafkaBrokerApiTest {

  static KafkaContainer kafka;
  static KafkaRestContainer kafkaRest;
  static KafkaRestContainer kafkaRestWithId;
  static GenericContainer<?> nginx;
  static String restApiUsername;
  static String restApiPassword;

  static final boolean AUTO_COMMIT_ENABLED = true;
  static final int TEST_NUM_SUBSCRIBERS = 1;
  static final String TEST_GROUP_ID = KafkaBrokerApiTest.class.getName();
  static final int TEST_POLLING_INTERVAL_MSEC = 100;
  static final String KAFKA_REST_ID = "kafka-rest-instance-0";
  private static final int TEST_THREAD_POOL_SIZE = 10;
  private static final String TEST_INSTANCE_ID = "test-instance-id";
  private static final TimeUnit TEST_TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final int TEST_TIMEOUT = 30;
  private static final int TEST_WAIT_FOR_MORE_MESSAGES_TIMEOUT = 5;

  private Injector injector;
  private KafkaSession session;
  private Gson gson;
  protected ClientType clientType;

  @Rule public TestName name = new TestName();

  public static class TestWorkQueue extends WorkQueue {

    @Inject
    public TestWorkQueue(IdGenerator idGenerator, MetricMaker metrics) {
      super(
          idGenerator,
          TEST_THREAD_POOL_SIZE,
          metrics,
          new PluginMapContext<>(
              DynamicMap.emptyMap(), PluginContext.PluginMetrics.DISABLED_INSTANCE));
    }
  }

  public static class TestModule extends AbstractModule {
    private KafkaProperties kafkaProperties;

    public TestModule(KafkaProperties kafkaProperties) {
      this.kafkaProperties = kafkaProperties;
    }

    @Override
    protected void configure() {
      bind(Gson.class)
          .annotatedWith(EventGson.class)
          .toProvider(EventGsonProvider.class)
          .in(Singleton.class);
      bind(MetricMaker.class).toInstance(mock(MetricMaker.class, Answers.RETURNS_DEEP_STUBS));
      bind(OneOffRequestContext.class)
          .toInstance(mock(OneOffRequestContext.class, Answers.RETURNS_DEEP_STUBS));

      bind(KafkaProperties.class).toInstance(kafkaProperties);
      bind(Log4JKafkaMessageLogger.class)
          .toInstance(mock(Log4JKafkaMessageLogger.class, Answers.RETURNS_DEEP_STUBS));
      bind(KafkaSession.class).in(Scopes.SINGLETON);

      bindKafkaClientImpl();

      bind(WorkQueue.class).to(TestWorkQueue.class);
    }

    protected void bindKafkaClientImpl() {
      bind(new TypeLiteral<Producer<String, String>>() {}).toProvider(KafkaProducerProvider.class);
      KafkaSubscriberProperties kafkaSubscriberProperties =
          new KafkaSubscriberProperties(
              TEST_POLLING_INTERVAL_MSEC, TEST_GROUP_ID, TEST_NUM_SUBSCRIBERS, ClientType.NATIVE);
      bind(KafkaSubscriberProperties.class).toInstance(kafkaSubscriberProperties);
    }
  }

  public static class TestConsumer implements ContextAwareConsumer<Event> {
    public final List<Event> messages = new ArrayList<>();
    private CountDownLatch[] locks;

    public TestConsumer(int numMessagesExpected) {
      resetExpectedMessages(numMessagesExpected);
    }

    public void resetExpectedMessages(int numMessagesExpected) {
      locks = new CountDownLatch[numMessagesExpected];
      for (int i = 0; i < numMessagesExpected; i++) {
        locks[i] = new CountDownLatch(i + 1);
      }
    }

    @Override
    public void accept(Event message, MessageContext messageContext) {
      messages.add(message);
      for (CountDownLatch countDownLatch : locks) {
        countDownLatch.countDown();
      }
    }

    public boolean await() {
      return await(locks.length);
    }

    public boolean await(int numItems) {
      return await(numItems, TEST_TIMEOUT, TEST_TIMEOUT_UNIT);
    }

    public boolean await(int numItems, long timeout, TimeUnit unit) {
      try {
        return locks[numItems - 1].await(timeout, unit);
      } catch (InterruptedException e) {
        return false;
      }
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    kafka = KafkaContainerProvider.get();
    kafka.start();
    kafkaRestWithId = new KafkaRestContainer(kafka, KAFKA_REST_ID, isAuthenticationProvided());
    kafkaRestWithId.start();
    kafkaRest = new KafkaRestContainer(kafka, isAuthenticationProvided());
    kafkaRest.start();

    System.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
  }

  @Before
  public void setup() {
    clientType = ClientType.NATIVE;
  }

  @AfterClass
  public static void afterClass() {
    stopContainer(kafka);
    stopContainer(kafkaRest);
    stopContainer(kafkaRestWithId);
    stopContainer(nginx);
  }

  private static void stopContainer(GenericContainer<?> container) {
    if (container != null) {
      container.stop();
    }
  }

  private static boolean isAuthenticationProvided() {
    return !Strings.isNullOrEmpty(restApiUsername) && !Strings.isNullOrEmpty(restApiPassword);
  }

  protected TestModule newTestModule(KafkaProperties kafkaProperties) {
    return new TestModule(kafkaProperties);
  }

  public void connectToKafka(KafkaProperties kafkaProperties) {
    Injector baseInjector = Guice.createInjector(newTestModule(kafkaProperties));
    WorkQueue testWorkQueue = baseInjector.getInstance(WorkQueue.class);
    KafkaSubscriberProperties kafkaSubscriberProperties =
        baseInjector.getInstance(KafkaSubscriberProperties.class);
    injector =
        baseInjector.createChildInjector(
            new KafkaApiModule(testWorkQueue, kafkaSubscriberProperties));
    session = injector.getInstance(KafkaSession.class);
    gson = injector.getInstance(Gson.class);

    session.connect();
  }

  @After
  public void teardown() {
    if (session != null) {
      session.disconnect();
    }
  }

  @Test
  public void shouldSendSyncAndReceiveToTopic() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    TestConsumer testConsumer = new TestConsumer(1);
    Event testEventMessage = new ProjectCreatedEvent();
    testEventMessage.instanceId = TEST_INSTANCE_ID;

    kafkaBrokerApi.receiveAsync(testTopic, testConsumer);
    kafkaBrokerApi.send(testTopic, testEventMessage);

    assertThat(testConsumer.await()).isTrue();
    assertThat(testConsumer.messages).hasSize(1);
    assertThat(gson.toJson(testConsumer.messages.get(0))).isEqualTo(gson.toJson(testEventMessage));

    assertNoMoreExpectedMessages(testConsumer);
  }

  private String testTopic() {
    return "test_topic_" + name.getMethodName();
  }

  @Test
  public void shouldSendAsyncAndReceiveToTopic() {
    connectToKafka(
        new KafkaProperties(
            true, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    TestConsumer testConsumer = new TestConsumer(1);
    Event testEventMessage = new ProjectCreatedEvent();
    testEventMessage.instanceId = TEST_INSTANCE_ID;

    kafkaBrokerApi.send(testTopic, testEventMessage);
    kafkaBrokerApi.receiveAsync(testTopic, testConsumer);

    assertThat(testConsumer.await()).isTrue();
    assertThat(testConsumer.messages).hasSize(1);
    assertThat(gson.toJson(testConsumer.messages.get(0))).isEqualTo(gson.toJson(testEventMessage));

    assertNoMoreExpectedMessages(testConsumer);
  }

  @Test
  public void shouldSendToTopicAndResetOffset() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    Event testEventMessage = new ProjectCreatedEvent();

    TestConsumer testConsumer = new TestConsumer(2);
    kafkaBrokerApi.receiveAsync(testTopic, testConsumer);

    kafkaBrokerApi.send(testTopic, testEventMessage);
    assertThat(testConsumer.await(1)).isTrue();
    assertThat(testConsumer.messages).hasSize(1);
    assertThat(gson.toJson(testConsumer.messages.get(0))).isEqualTo(gson.toJson(testEventMessage));

    kafkaBrokerApi.replayAllEvents(testTopic);
    assertThat(testConsumer.await(2)).isTrue();
    assertThat(testConsumer.messages).hasSize(2);
    assertThat(gson.toJson(testConsumer.messages.get(1))).isEqualTo(gson.toJson(testEventMessage));
  }

  @Test
  public void shouldConsumerWithGroupIdConsumeMessage() {
    connectToKafka(
        new KafkaProperties(
            true, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    TestConsumer testConsumer = new TestConsumer(1);
    Event testEventMessage = new ProjectCreatedEvent();
    testEventMessage.instanceId = TEST_INSTANCE_ID;

    kafkaBrokerApi.send(testTopic, testEventMessage);
    kafkaBrokerApi.receiveAsync(testTopic, "group-id-1", testConsumer);

    assertThat(testConsumer.await()).isTrue();
    assertThat(testConsumer.messages).hasSize(1);
    assertThat(gson.toJson(testConsumer.messages.get(0))).isEqualTo(gson.toJson(testEventMessage));

    assertNoMoreExpectedMessages(testConsumer);
  }

  @Test
  public void shouldRegisterConsumerWithoutExternalGroupId() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    TestConsumer testConsumer = new TestConsumer(1);

    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId()).isEmpty();
    kafkaBrokerApi.receiveAsync(testTopic, testConsumer);
    assertThat(kafkaBrokerApi.topicSubscribers())
        .containsExactly(topicSubscriber(testTopic, testConsumer));
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId()).isEmpty();
  }

  @Test
  public void shouldRegisterConsumerWithExternalGroupId() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    String groupId = "group_id_1";
    TestConsumer testConsumer = new TestConsumer(1);

    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId()).isEmpty();
    kafkaBrokerApi.receiveAsync(testTopic, groupId, testConsumer);
    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId())
        .containsExactly(
            topicSubscriberWithGroupId(groupId, topicSubscriber(testTopic, testConsumer)));
  }

  @Test
  public void shouldRegisterDifferentConsumersWithTheSameExternalGroupId() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    String groupId = "group_id_1";
    TestConsumer testConsumer1 = new TestConsumer(1);
    TestConsumer testConsumer2 = new TestConsumer(1);

    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId()).isEmpty();
    kafkaBrokerApi.receiveAsync(testTopic, groupId, testConsumer1);
    kafkaBrokerApi.receiveAsync(testTopic, groupId, testConsumer2);
    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId())
        .containsExactly(
            topicSubscriberWithGroupId(groupId, topicSubscriber(testTopic, testConsumer1)),
            topicSubscriberWithGroupId(groupId, topicSubscriber(testTopic, testConsumer2)));
  }

  @Test
  public void shouldRegisterConsumerWithConfiguredGroupIdAndConsumerWithExternalGroupId() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    String groupId = "group_id_1";
    TestConsumer testConsumer1 = new TestConsumer(1);
    TestConsumer testConsumer2 = new TestConsumer(1);

    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId()).isEmpty();
    kafkaBrokerApi.receiveAsync(testTopic, testConsumer1);
    kafkaBrokerApi.receiveAsync(testTopic, groupId, testConsumer2);
    assertThat(kafkaBrokerApi.topicSubscribers())
        .containsExactly(topicSubscriber(testTopic, testConsumer1));

    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId())
        .containsExactly(
            topicSubscriberWithGroupId(groupId, topicSubscriber(testTopic, testConsumer2)));
  }

  @Test
  public void shouldNotRegisterTheSameConsumerWithExternalGroupIdTwicePerTopic() {
    connectToKafka(
        new KafkaProperties(
            false, clientType, getKafkaRestApiUriString(), restApiUsername, restApiPassword));
    KafkaBrokerApi kafkaBrokerApi = injector.getInstance(KafkaBrokerApi.class);
    String testTopic = testTopic();
    String groupId = "group_id_1";
    TestConsumer testConsumer = new TestConsumer(1);

    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId()).isEmpty();
    kafkaBrokerApi.receiveAsync(testTopic, groupId, testConsumer);
    kafkaBrokerApi.receiveAsync(testTopic, groupId, testConsumer);
    assertThat(kafkaBrokerApi.topicSubscribers()).isEmpty();
    assertThat(kafkaBrokerApi.topicSubscribersWithGroupId())
        .containsExactly(
            topicSubscriberWithGroupId(groupId, topicSubscriber(testTopic, testConsumer)));
  }

  protected String getKafkaRestApiUriString() {
    return null;
  }

  private void assertNoMoreExpectedMessages(TestConsumer testConsumer) {
    testConsumer.resetExpectedMessages(1);
    assertThat(testConsumer.await(1, TEST_WAIT_FOR_MORE_MESSAGES_TIMEOUT, TEST_TIMEOUT_UNIT))
        .isFalse();
  }
}
