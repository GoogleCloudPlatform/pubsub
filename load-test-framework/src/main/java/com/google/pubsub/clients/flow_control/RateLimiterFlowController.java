package com.google.pubsub.clients.flow_control;

import com.google.common.util.concurrent.RateLimiter;

/**
 * A FlowController that delegates to a guava RateLimiter for a static allowed rate.
 */
public class RateLimiterFlowController implements FlowController {
    private final RateLimiter limiter;
   public RateLimiterFlowController(double rate) {
        this.limiter = RateLimiter.create(rate);
    }
    @Override
    public void requestStart() {
        limiter.acquire();
    }

    @Override
    public void informFinished(boolean wasSuccessful) {}
}