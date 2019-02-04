package com.google.pubsub.flic.controllers;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class KafkaResourceController extends ResourceController {
    protected static final Logger log = LoggerFactory.getLogger(KafkaResourceController.class);
    private final String topic;

    KafkaResourceController(String topic, ScheduledExecutorService executor) {
        super(executor);
        this.topic = topic;
    }

    @Override
    protected void startAction() throws Exception {
        if (!Client.zookeeperIpAddress.isEmpty()) {
            ZkClient zookeeperClient = new ZkClient(Client.zookeeperIpAddress, 15000, 10000,
                    ZKStringSerializer$.MODULE$);
            ZkUtils zookeeperUtils = new ZkUtils(zookeeperClient,
                    new ZkConnection(Client.zookeeperIpAddress), false);
            try {
                deleteTopic(zookeeperUtils);
                AdminUtils
                        .createTopic(zookeeperUtils, topic, Client.partitions, Client.replicationFactor,
                                AdminUtils.createTopic$default$5(),
                                AdminUtils.createTopic$default$6());
                log.info("Created topic " + topic + ".");
            } finally {
                zookeeperClient.close();
            }
        }
    }

    private void deleteTopic(ZkUtils zookeeperUtils) throws Exception {
        if (AdminUtils.topicExists(zookeeperUtils, topic)) {
            log.info("Deleting topic " + topic + ".");
            AdminUtils.deleteTopic(zookeeperUtils, topic);
        } else {
            log.info("Topic " + topic + " does not exist.");
        }
        while (AdminUtils.topicExists(zookeeperUtils, topic)) {
            Thread.sleep(10);
        }
    }

    @Override
    protected void stopAction() throws Exception {
        if (!Client.zookeeperIpAddress.isEmpty()) {
            ZkClient zookeeperClient = new ZkClient(Client.zookeeperIpAddress, 15000, 10000,
                    ZKStringSerializer$.MODULE$);
            ZkUtils zookeeperUtils = new ZkUtils(zookeeperClient,
                    new ZkConnection(Client.zookeeperIpAddress), false);
            try {
                deleteTopic(zookeeperUtils);
            } finally {
                zookeeperClient.close();
            }
        }
    }
}
