package com.google.pubsub.driver;

import java.util.Properties;

import com.google.pubsub.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by gewillovic on 8/4/17.
 */
public class Main {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("project", "dataproc-kafka-test");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    System.out.println("PUBLISHING FROM MAIN THREAD");

    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord<>(
          "wow", 0, Integer.toString(i), "message" + Integer.toString(i)));
      System.out.println(Thread.currentThread().getName() + " " + i);
    }

    new Thread(new Runnable() {
      @Override
      public void run() {
        System.out.println("PUBLISHING FROM ALT THREAD");
        for (int i = 0; i < 10; i++) {
          producer.send(new ProducerRecord<>(
              "wow1", 0, Integer.toString(i), "message" + Integer.toString(i)));
          System.out.println(Thread.currentThread().getName() + " " + i);
        }
      }
    }).start();

    new Thread(new Runnable() {
      @Override
      public void run() {
        System.out.println("FLUSHING RECORDS");
        producer.flush();
        while(true);
      }
    }).start();
  }
}
