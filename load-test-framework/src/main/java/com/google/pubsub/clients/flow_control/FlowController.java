package com.google.pubsub.clients.flow_control;

public interface FlowController {
    /**
     * Request starting a flow controlled action, block until allowed.
     */
    void requestStart();

    /**
     * Inform the FlowController that an action has finished.
     */
    void informFinished(boolean wasSuccessful);
}