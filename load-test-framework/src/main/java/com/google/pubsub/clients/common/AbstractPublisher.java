package com.google.pubsub.clients.common;

import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.clients.flow_control.OutstandingCountFlowController;
import com.google.pubsub.clients.flow_control.FlowController;
import com.google.pubsub.clients.flow_control.RateLimiterFlowController;
import com.google.pubsub.flic.common.LoadtestProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;

public abstract class AbstractPublisher extends PooledWorkerTask {
    private static final Logger log = LoggerFactory.getLogger(AbstractPublisher.class);
    private static final LogEveryN logEveryN = new LogEveryN(log, 500);

    private final ByteString payload;
    private final double perThreadRateUpperBound;
    // Start at 100 kB/s/thread
    private static final double STARTING_PER_THREAD_BYTES_PER_SEC = Math.pow(10, 5);
    private final boolean flowControlLimitExists;
    private final double startingPerThreadRate;

    public AbstractPublisher(LoadtestProto.StartRequest request, MetricsHandler handler, int workerCount) {
        super(request, handler, workerCount);
        this.payload = createMessage(request.getPublisherOptions().getMessageSize());
        if (request.getPublisherOptions().getRate() <= 0) {
            this.perThreadRateUpperBound = Double.MAX_VALUE;
            this.flowControlLimitExists = false;
            this.startingPerThreadRate = STARTING_PER_THREAD_BYTES_PER_SEC / payload.size();
            log.info("Per thread rate to start: " + startingPerThreadRate);
        } else {
            this.perThreadRateUpperBound = request.getPublisherOptions().getRate() / workerCount;
            this.flowControlLimitExists = true;
            this.startingPerThreadRate = 0;
            log.info("Per thread rate upper bound: " + perThreadRateUpperBound);
        }
    }

    protected ByteString getPayload() {
        return payload;
    }

    /**
     * Creates a string message of a certain size.
     */
    private ByteString createMessage(int msgSize) {
        byte[] payloadArray = new byte[msgSize];
        Arrays.fill(payloadArray, (byte) 'A');
        return ByteString.copyFrom(payloadArray);
    }

    @Override
    protected void startAction() {
        int id = (new Random()).nextInt();
        int sequence_number = 0;
        FlowController flowController;
        if (flowControlLimitExists) {
            flowController = new RateLimiterFlowController(perThreadRateUpperBound);
        } else {
            flowController = new OutstandingCountFlowController(startingPerThreadRate);
        }
        while (!isShutdown.get()) {
            int permits = flowController.requestStart();
            for (int i = 0; i < permits; i++) {
                long publishTimestampMillis = System.currentTimeMillis();
                LoadtestProto.MessageIdentifier identifier =
                        LoadtestProto.MessageIdentifier.newBuilder()
                                .setSequenceNumber(sequence_number)
                                .setPublisherClientId(id)
                                .build();
                Futures.addCallback(
                        publish(id, sequence_number++, publishTimestampMillis),
                        new FutureCallback<Void>() {
                            public void onSuccess(Void result) {
                                metricsHandler.add(
                                        identifier, Duration.ofMillis(
                                                System.currentTimeMillis() - publishTimestampMillis));
                                flowController.informFinished(true);
                            }

                            public void onFailure(Throwable t) {
                                metricsHandler.addFailure();
                                flowController.informFinished(false);
                                logEveryN.error("Publisher error: " + t);
                            }
                        }, MoreExecutors.directExecutor());
            }
        }
    }


    protected abstract ListenableFuture<Void> publish(
            int clientId, int sequenceNumber, long publishTimestampMillis);
}
