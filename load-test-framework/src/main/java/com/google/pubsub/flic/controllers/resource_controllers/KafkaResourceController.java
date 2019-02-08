package com.google.pubsub.flic.controllers.resource_controllers;

import com.google.pubsub.flic.common.KafkaFlags;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class KafkaResourceController extends ResourceController {
    private static final Logger log = LoggerFactory.getLogger(KafkaResourceController.class);
    private final String topic;

    public KafkaResourceController(String topic, ScheduledExecutorService executor) {
        super(executor);
        this.topic = topic;
    }

    @Override
    protected void startAction() throws Exception {
        ZkClient zookeeperClient = new ZkClient(
                KafkaFlags.getInstance().zookeeperIp, 15000, 10000, ZKStringSerializer$.MODULE$);
        ZkUtils zookeeperUtils = new ZkUtils(zookeeperClient,
                new ZkConnection(KafkaFlags.getInstance().zookeeperIp), false);
        try {
            deleteTopic(zookeeperUtils);
            AdminUtils
                    .createTopic(
                            zookeeperUtils, topic, KafkaFlags.getInstance().partitions,
                            KafkaFlags.getInstance().replicationFactor,
                            AdminUtils.createTopic$default$5(),
                            AdminUtils.createTopic$default$6());
            log.info("Created topic " + topic + ".");
        } finally {
            zookeeperClient.close();
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
        ZkClient zookeeperClient = new ZkClient(KafkaFlags.getInstance().zookeeperIp, 15000, 10000,
                ZKStringSerializer$.MODULE$);
        ZkUtils zookeeperUtils = new ZkUtils(zookeeperClient,
                new ZkConnection(KafkaFlags.getInstance().zookeeperIp), false);
        try {
            deleteTopic(zookeeperUtils);
        } finally {
            zookeeperClient.close();
        }
    }
}
