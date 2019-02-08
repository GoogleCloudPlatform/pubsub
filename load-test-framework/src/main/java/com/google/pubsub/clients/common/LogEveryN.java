package com.google.pubsub.clients.common;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class LogEveryN {
    private final Logger logger;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int n;

    public LogEveryN(Logger logger, int n) {
        this.logger = logger;
        this.n = n;
    }

    public void error(String toLog) {
        int previous = counter.getAndUpdate(existing -> (existing + 1) % n);
        if (previous == 0) {
            logger.error(toLog);
        }
    }
}
