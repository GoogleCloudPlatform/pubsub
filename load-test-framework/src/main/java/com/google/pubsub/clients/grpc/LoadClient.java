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
package com.google.pubsub.clients.grpc;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.*;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.LoadTestParams;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.ProjectInfo;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.PublishResponseResult;
import com.google.pubsub.clients.grpc.PubsubLoadClientAdapter.PullResponseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * A high performance load test client for Cloud Pub/Sub. That supports gRPC as underlying transport methods.
 */
public class LoadClient {
  private static final Logger log = LoggerFactory.getLogger(LoadClient.class);
  private static final int REQUEST_FAILED_CODE = -1;  // A client side error occurred.
  private final int secondsToRun;
  private final String project;
  private final String topicName;
  private final String subscriptionName;
  private final RateLimiter publishRateLimiter;
  private final RateLimiter pullRateLimiter;
  private final Semaphore concurrentPublishLimiter;
  private final Semaphore concurrentPullLimiter;
  private final LoadTestStats publishStats;
  private final LoadTestStats pullStats;
  private final ScheduledExecutorService executorService;
  private final MetricsHandler metricsHandler;
  private final PubsubLoadClientAdapter pubsubClient;
  private final boolean enableMetricReporting;
  private String topicPath;
  private String subscriptionPath;

  private LoadClient(Builder builder) throws IOException {
    secondsToRun = builder.secondsToRun;
    project = Preconditions.checkNotNull(builder.project);
    topicName = Preconditions.checkNotNull(builder.topic);
    topicPath = "projects/" + project + "/topics/" + topicName;
    subscriptionName = Preconditions.checkNotNull(builder.subscription);
    subscriptionPath = "projects/" + project + "/subscriptions/" + subscriptionName;
    publishRateLimiter = RateLimiter.create(builder.publishRequestsRateLimit);
    pullRateLimiter = RateLimiter.create(builder.pullRequestsRateLimit);
    concurrentPublishLimiter = new Semaphore(builder.maxConcurrentPublishRequests, false);
    concurrentPullLimiter = new Semaphore(builder.maxConcurrentPullRequests, false);
    enableMetricReporting = builder.enableMetricsReporting;

    int connections = builder.maxConcurrentPublishRequests + builder.maxConcurrentPullRequests;
    executorService =
        Executors.newScheduledThreadPool(
            connections + 10,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("load-thread").build());
    publishStats = new LoadTestStats("publish");
    pullStats = new LoadTestStats("pull");

    AccessTokenProvider accessTokenProvider = new AccessTokenProvider();
    metricsHandler =
        new MetricsHandler(project, builder.metricsReportIntervalSecs, accessTokenProvider);
    metricsHandler.initialize();

    ProjectInfo projectInfo = new ProjectInfo(project, topicName, subscriptionName);
    LoadTestParams loadTestParams =
        new LoadTestParams(
            "pubsub.googleapis.com",
            builder.publishBatchSize,
            builder.messageSize,
            builder.pullBatchSize,
            builder.maxConcurrentPublishRequests,
            builder.maxConcurrentPullRequests,
            builder.requestDeadlineMillis);

    pubsubClient = new PubsubGrpcLoadClient(
        accessTokenProvider,
        projectInfo,
        loadTestParams);


    log.info(
        "Load test configured:"
            + "\n\tseconds to run: " + builder.secondsToRun
            + "\n\tproject: " + builder.project
            + "\n\ttopic: " + builder.topic
            + "\n\tsubscription: " + builder.subscription
            + "\n\tmessage size: " + builder.messageSize
            + "\n\trequest deadline milliseconds: " + builder.requestDeadlineMillis
            + "\n\tmax. publish rate: " + builder.publishRequestsRateLimit
            + "\n\tmax. concurrent publish requests: " + builder.maxConcurrentPublishRequests
            + "\n\tpublish batch size: " + builder.publishBatchSize
            + "\n\tmax. concurrent pull requests: " + builder.maxConcurrentPullRequests
            + "\n\tpull batch size: " + builder.pullBatchSize);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static void main(String[] args) throws Exception {

    Builder builder = LoadClient.builder();
    new JCommander(builder, args);
    LoadClient loadClient = builder.build();
    // Hangs until done.
    loadClient.start();

    log.info("Closing all - good bye!");
  }

  public void start() throws InterruptedException, ExecutionException {
    // Try to create the topic and wait.
    pubsubClient.createTopic(topicName).get();

    // Try to create the subscription and wait.
    pubsubClient
        .createSubscription(subscriptionName, "projects/" + project + "/topics/" + topicName)
        .get();

    startLoad();

    executorService.scheduleAtFixedRate(() -> {
          log.info("Printing stats");
          publishStats.printStats();
          pullStats.printStats();
        },
        5, 10, TimeUnit.SECONDS);

    executorService.awaitTermination(
        secondsToRun == -1 ? Integer.MAX_VALUE : secondsToRun, TimeUnit.SECONDS);

    executorService.shutdownNow();
    publishStats.printStats();
    pullStats.printStats();
  }

  private void startLoad() {
    publishStats.startTimer();
    executorService.submit(
        new Runnable() {
          @Override
          public void run() {
            while (true) {
              publishRateLimiter.acquire();
              concurrentPublishLimiter.acquireUninterruptibly();
              final Stopwatch stopWatch = Stopwatch.createStarted();
              ListenableFuture<PublishResponseResult> publishFuture =
                  pubsubClient.publishMessages(topicPath);
              Futures.addCallback(
                  publishFuture,
                  new FutureCallback<PublishResponseResult>() {
                    @Override
                    public void onSuccess(PublishResponseResult result) {
                      long elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
                      concurrentPublishLimiter.release();
                      if (result.isOk()) {
                        publishStats.recordSuccessfulRequest(
                            result.getMessagesPublished(), elapsed);
                        if (enableMetricReporting) {
                          metricsHandler.recordPublishAckLatency(elapsed);
                        }
                      } else {
                        publishStats.recordFailedRequest();
                      }
                      if (enableMetricReporting) {
                        metricsHandler.recordRequestCount(
                            topicName,
                            Operation.PUBLISH.toString(),
                            result.getStatusCode(),
                            result.getMessagesPublished());
                      }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                      concurrentPublishLimiter.release();
                      log.warn(
                          "Unable to execute a publish request (client-side error)", t);
                      publishStats.recordFailedRequest();
                      if (enableMetricReporting) {
                        metricsHandler.recordRequestCount(
                            topicName, Operation.PUBLISH.toString(), REQUEST_FAILED_CODE, 0);
                      }
                    }
                  });
            }
          }
        });
    pullStats.startTimer();

    executorService.submit(
        new Runnable() {
          @Override
          public void run() {
            while (true) {
              concurrentPullLimiter.acquireUninterruptibly();
              pullRateLimiter.acquire();
              final Stopwatch stopWatch = Stopwatch.createStarted();
              ListenableFuture<PullResponseResult> pullFuture =
                  pubsubClient.pullMessages(subscriptionPath);
              Futures.addCallback(
                  pullFuture,
                  new FutureCallback<PullResponseResult>() {
                    @Override
                    public void onSuccess(PullResponseResult result) {
                      concurrentPullLimiter.release();
                      recordPullResponseResult(
                          result, Operation.PULL, stopWatch.elapsed(TimeUnit.MILLISECONDS));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                      log.warn(
                          "Unable to execute a pull request (client-side error)", t);
                      if (enableMetricReporting) {
                        metricsHandler.recordRequestCount(
                            subscriptionName, Operation.PULL.toString(), REQUEST_FAILED_CODE, 0);
                      }
                    }
                  });
            }
          }
        });

    if (enableMetricReporting) {
      metricsHandler.startReporting();
    }
  }

  private void recordPullResponseResult(
      PullResponseResult result, Operation pullOperation, long elapsed) {
    Preconditions.checkArgument(pullOperation == Operation.PULL);
    if (result.isOk()) {
      pullStats.recordSuccessfulRequest(result.getMessagesPulled(), elapsed);
      if (enableMetricReporting) {
        result.getEndToEndLatenciesMillis().forEach(metricsHandler::recordEndToEndLatency);
      }
    } else {
      pullStats.recordFailedRequest();
    }
    metricsHandler.recordRequestCount(
        topicName,
        pullOperation.toString(),
        result.getStatusCode(),
        result.getMessagesPulled());
  }

  private enum Operation {
    PUBLISH,
    PULL,
  }

  /**
   * Builder of {@link LoadClient}.
   */
  public static class Builder {
    @Parameter(
        names = {"--project"},
        description = "Name for the cloud project to target for the test."
    )
    String project = "cloud-pubsub-load-tests";
    @Parameter(
        names = {"--topic"},
        required = true,
        description = "Name of the topic to target for the tests."
    )
    String topic = "big-loadtest";
    @Parameter(
        names = {"--subscription"},
        description = "Name for the subscription to target for the test."
    )
    String subscription = "big-load-subscriber-sub-pull-0";
    @Parameter(
        names = {"--message_size"},
        description = "Number of bytes per message."
    )
    int messageSize = 1000;  // 1 KB
    @Parameter(names = {"--publish_batch_size"},
        description = "Number of messages per publish call.")
    int publishBatchSize = 1;
    @Parameter(names = {"--pull_batch_size"},
        description = "Number of messages per pull call.")
    int pullBatchSize = 1;
    @Parameter(names = {"--publish_requests_rate_limit"},
        description = "Maximum rate per second of publish requests.")
    int publishRequestsRateLimit = 1;
    @Parameter(names = {"--pull_requests_rate_limit"},
        description = "Maximum rate per second of pull requests.")
    int pullRequestsRateLimit = 1;
    @Parameter(names = {"--max_concurrent_publish_request"},
        description = "Maximum number of concurrent publish requests.")
    int maxConcurrentPublishRequests = 1;
    @Parameter(names = {"--max_concurrent_pull_request"},
        description = "Maximum number of concurrent pull requests.")
    int maxConcurrentPullRequests = 1;
    @Parameter(names = {"--seconds_to_run"},
        description = "Number of seconds to run the load test. Use -1 to never stop.")
    int secondsToRun = -1;
    @Parameter(names = {"--metrics_report_interval_secs"},
        description = "Number of wait in between reporting the latest metric numbers.")
    int metricsReportIntervalSecs = 30;
    @Parameter(names = {"--enable_metrics_report"}, description = "Enable metrics reporting.")
    boolean enableMetricsReporting = true;
    @Parameter(names = {"--request_deadline_millis"}, description = "Request deadline in miliseconds.")
    int requestDeadlineMillis = 10000;

    private Builder() {
    }

    LoadClient build() throws IOException {
      return new LoadClient(this);
    }
  }
}

