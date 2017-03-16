package com.google.pubsub.clients.mapped;

import com.beust.jcommander.JCommander;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler.MetricName;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.producer.PubsubProducer;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that publishes messages utilizing Pub/Sub's implementation of the Kafka Producer<K,V>
 * interface
 */
public class CPSPublisherTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private final String topic;
  private final String payload;
  private final int batchSize;
  private final PubsubProducer<String, String> publisher;

  @SuppressWarnings("unchecked")
  private CPSPublisherTask(StartRequest request) {
    super(request, "mapped", MetricName.PUBLISH_ACK_LATENCY);
    this.topic = request.getTopic();
    this.payload = LoadTestRunner.createMessage(request.getMessageSize());
    this.batchSize = request.getPublishBatchSize();

    this.publisher = new PubsubProducer.Builder<>(request.getProject(), new StringSerializer(),
        new StringSerializer())
        .batchSize(9500000L)
        .lingerMs(Durations.toMillis(request.getPublishBatchDuration()))
        .bufferMemory(1000000000)
        .isAcks(true)
        .build();
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
          new ProducerRecord<>(topic, null, System.currentTimeMillis(), null, payload),
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
  public void shutdown() { publisher.close(); }
}
