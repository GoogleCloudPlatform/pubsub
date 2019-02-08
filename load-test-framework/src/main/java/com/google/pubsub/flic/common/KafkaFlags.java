package com.google.pubsub.flic.common;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class KafkaFlags {
    @Parameter(
            names = {"--broker"},
            description = "The network address of the Kafka broker."
    )
    public String broker;

    @Parameter(
            names = {"--zookeeper_ip"},
            description = "The network address of the Zookeeper client."
    )
    public String zookeeperIp;

    @Parameter(
            names = {"--replication_factor"},
            description = "The replication factor to use in creating a Kafka topic, if the Kafka broker"
                    + "is included as a flag."
    )
    public int replicationFactor = 2;

    @Parameter(
            names = {"--partitions"},
            description = "The number of partitions to use in creating a Kafka topic, if the "
                    + "Kafka broker is included as a flag."
    )
    public int partitions = 100;

    private KafkaFlags() {
    }

    private static final KafkaFlags KAFKA_FLAGS_INSTANCE = new KafkaFlags();

    public static KafkaFlags getInstance() {
        return KAFKA_FLAGS_INSTANCE;
    }
}
