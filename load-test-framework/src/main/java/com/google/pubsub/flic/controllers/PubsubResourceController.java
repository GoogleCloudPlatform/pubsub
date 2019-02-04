package com.google.pubsub.flic.controllers;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A ResourceController that manages cloud pubsub topics and subscriptions.
 */
public class PubsubResourceController extends ResourceController {
    protected static final Logger log = LoggerFactory.getLogger(PubsubResourceController.class);
    private final String project;
    private final String topic;
    private final List<String> subscriptions;
    private final Pubsub pubsub;

    PubsubResourceController(
            String project, String topic, List<String> subscriptions, ScheduledExecutorService executor,
            Pubsub pubsub) {
        super(executor);
        this.project = project;
        this.topic = topic;
        this.subscriptions = subscriptions;
        this.pubsub = pubsub;
    }
    @Override
    protected void startAction() throws Exception {
        try {
            pubsub.projects().topics()
                    .create("projects/" + project + "/topics/" + topic, new Topic()).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
                log.error("Error creating subscription");
                throw e;
            }
            log.info("Topic already exists, reusing.");
        }
        for (String subscription: subscriptions) {
            log.error("Creating subscription: " + subscription.toString());
            try {
                pubsub.projects().subscriptions().delete("projects/" + project
                        + "/subscriptions/" + subscription).execute();
            } catch (IOException e) {
                log.debug("Error deleting subscription, assuming it has not yet been created.", e);
            }
            pubsub.projects().subscriptions().create("projects/" + project
                    + "/subscriptions/" + subscription, new Subscription()
                    .setTopic("projects/" + project + "/topics/" + topic)
                    .setAckDeadlineSeconds(10)).execute();
        }
    }

    @Override
    protected void stopAction() throws Exception {
        for (String subscription: subscriptions) {
            pubsub.projects().subscriptions().delete("projects/" + project
                    + "/subscriptions/" + subscription).execute();
        }
        pubsub.projects().topics()
                .delete("projects/" + project + "/topics/" + topic).execute();
    }
}
