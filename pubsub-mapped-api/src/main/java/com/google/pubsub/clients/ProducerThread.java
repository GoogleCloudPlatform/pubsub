package com.google.pubsub.clients;

import com.google.pubsub.clients.producer.PubsubProducer;
import com.google.pubsub.clients.producer.PubsubProducer.Builder;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;


public class ProducerThread implements Runnable {
  private String command;
  private PubsubProducer producer;
  private String topic;
  private static final Logger log = LoggerFactory.getLogger(ProducerThread.class);

  public ProducerThread(String s, Properties props, String topic) throws IOException {
    this.command = s;
    this.producer = new Builder<>(props.getProperty("project"), new StringSerializer(), new StringSerializer())
        .batchSize(Integer.parseInt(props.getProperty("batch.size")))
        .isAcks(props.getProperty("acks").matches("1|all"))
    .build();
    this.topic = topic;
  }

  public void run() {
    log.info("Start running the command");
    processCommand();
    log.info("End running the command");
  }

  private void processCommand() {
    try {
      ProducerRecord<String, String> msg = new ProducerRecord<>(topic, "hello" + command);
      for (int i = 0; i < 1; i++) {
        producer.send(
            msg,
            new Callback() {
              public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                  log.error("Exception sending the message: " + exception.getMessage());
                } else {
                  log.info("Successfully sent message");
                }
              }
            }
        );
      }
      Thread.sleep(5000);
      producer.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public String toString() {
    return command;
  }
}