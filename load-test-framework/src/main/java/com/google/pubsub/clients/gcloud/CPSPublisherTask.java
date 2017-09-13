// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.clients.gcloud;

/**
 * Runs a task that publishes messages to a Cloud Pub/Sub topic.
 */
class CPSPublisherTask /*extends Task*/ {
  /*private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private final Publisher publisher;
  private final ByteString payload;
  private final int batchSize;
  private final Integer id;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private CPSPublisherTask(StartRequest request) {
    super(request, "gcloud", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    try {
      this.publisher =
          Publisher.newBuilder(TopicName.create(request.getProject(), request.getTopic()))
              *//*.setBundlingSettings(
                  BundlingSettings.newBuilder()
                      .setDelayThreshold(
                          Duration.millis(Durations.toMillis(request.getPublishBatchDuration())))
                      .setRequestByteThreshold(9500000L)
                      .setElementCountThreshold(950L)
                      .build())
              .setFlowControlSettings(
                  FlowControlSettings.newBuilder()
                      .setMaxOutstandingRequestBytes(1000000000)
                      .build()) // 1 GB*//*
              .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.payload = ByteString.copyFromUtf8(LoadTestRunner.createMessage(request.getMessageSize()));
    this.batchSize = request.getPublishBatchSize();
    this.id = (new Random()).nextInt();
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSPublisherTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    try {
      AtomicInteger numPending = new AtomicInteger(batchSize);
      final SettableFuture<RunResult> done = SettableFuture.create();
      String sendTime = String.valueOf(System.currentTimeMillis());
      for (int i = 0; i < batchSize; i++) {
        publisher
            .publish(
                PubsubMessage.newBuilder()
                    .setData(payload)
                    .putAttributes("sendTime", sendTime)
                    .putAttributes("clientId", id.toString())
                    .putAttributes(
                        "sequenceNumber", Integer.toString(sequenceNumber.getAndIncrement()))
                    .build())
            *//*.addCallback(
                new RpcFutureCallback<String>() {
                  @Override
                  public void onSuccess(String s) {
                    if (numPending.decrementAndGet() == 0) {
                      done.set(RunResult.fromBatchSize(batchSize));
                    }
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    done.setException(t);
                  }
                })*//*;
      }
      return done;
    } catch (Throwable t) {
      log.error("Flow control error.", t);
      return Futures.immediateFailedFuture(t);
    }
  }

  @Override
  protected void shutdown() {
    try {
      publisher.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }*/
}
