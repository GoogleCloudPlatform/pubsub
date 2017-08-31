package com.google.pubsub.clients.mapped;

import com.beust.jcommander.JCommander;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.producer.KafkaProducer;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.clients.common.MetricsHandler.MetricName;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that publishes messages utilizing Pub/Sub's implementation of the Kafka Producer<K,V> Interface.
 */
public class CPSPublisherTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);

  private final String topic;
  private final String payload;

  private final int batchSize;
  private final KafkaProducer<String, String> publisher;

  @SuppressWarnings("unchecked")
  private CPSPublisherTask(StartRequest request) {
    super(request, "mapped", MetricName.PUBLISH_ACK_LATENCY);

    this.topic = request.getTopic();
    this.batchSize = request.getPublishBatchSize();
    this.payload = LoadTestRunner.createMessage(request.getMessageSize());

    Properties props = new Properties();
    props.put("project", "dataproc-kafka-test");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    this.publisher = new KafkaProducer<>(props);
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();

    new JCommander(options, args);

    LoadTestRunner.run(options, CPSPublisherTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    SettableFuture<RunResult> result = SettableFuture.create();

    AtomicInteger messagesToSend = new AtomicInteger(batchSize);
    AtomicInteger messagesSentSuccess = new AtomicInteger(batchSize);

    for (int i = 0; i < batchSize; i++) {
      publisher.send(
          new ProducerRecord<>(topic, 0, "", payload),
          (metadata, exception) -> {
            if (exception != null) {
              messagesSentSuccess.decrementAndGet();
              log.error(exception.getMessage(), exception);
            }
            if (messagesToSend.decrementAndGet() == 0) {
              result.set(RunResult.fromBatchSize(messagesSentSuccess.get()));
            }
          });
    }
    return result;
  }

  @Override
  public void shutdown() {
    publisher.close();
  }
}
