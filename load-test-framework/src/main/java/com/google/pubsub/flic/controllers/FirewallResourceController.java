package com.google.pubsub.flic.controllers;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Firewall;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

public class FirewallResourceController extends ResourceController {
    private static final Logger log = LoggerFactory.getLogger(FirewallResourceController.class);
    private static String FIREWALL_NAME = "cloud-loadtest-framework-firewall-rule";

    private final String project;
    private final Compute compute;

    FirewallResourceController(String project, ScheduledExecutorService executor, Compute compute) {
        super(executor);
        this.project = project;
        this.compute = compute;
    }

    @Override
    protected void startAction() throws Exception {
        log.info("Creating firewall");
        Firewall firewallRule = new Firewall()
                .setName(FIREWALL_NAME)
                .setDescription("A firewall rule to allow the driver to coordinate load test instances.")
                .setAllowed(ImmutableList.of(
                        new Firewall.Allowed()
                                .setIPProtocol("tcp")
                                .setPorts(Collections.singletonList("5000"))));
        try {
            compute.firewalls().insert(project, firewallRule).execute();
        } catch (GoogleJsonResponseException e) {
            log.info("Firewall error: " + e);
            if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
                throw e;
            }
            //compute.firewalls()
            //        .update(project, "cloud-loadtest-framework-firewall-rule", firewallRule).execute();
        }
        log.info("Existing firewalls:");
        for (Firewall firewall: compute.firewalls().list(project).execute().getItems()) {
            log.info(firewall.toPrettyString());
        }
    }

    @Override
    protected void stopAction() throws Exception {
        compute.firewalls().delete(project, FIREWALL_NAME);
    }
}
