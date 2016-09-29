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
package com.google.pubsub.clients;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.*;
import com.google.pubsub.clients.PubsubLoadClientAdapter.LoadTestParams;
import com.google.pubsub.clients.PubsubLoadClientAdapter.ProjectInfo;
import com.google.pubsub.clients.PubsubLoadClientAdapter.PublishResponseResult;
import com.google.pubsub.clients.PubsubLoadClientAdapter.PullResponseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * A high performance load test client for Cloud Pub/Sub. That supports gRPC as underlying transport methods.
 */
public class LoadClient {
  @Parameter(
      names = {"--environment"},
      description =
          "Name for the cloud pubsub environment. Either: LOADTEST, QA, STAGING, PROD or EXPERIMENTAL"
  )
  public static final Environment environmentFlag = Environment.LOADTEST;
  @Parameter(
      names = {"--project"},
      description = "Name for the cloud project to target for the test."
  )
  public static final String projectFlag = "cloud-pubsub-load-tests";
  @Parameter(
      names = {"--topic"},
      required = true,
      description = "Name of the topic to target for the tests."
  )
  public static final String topicFlag = "big-loadtest";
  @Parameter(
      names = {"--subscription"},
      description = "Name for the subscription to target for the test."
  )
  public static final String subscriptionFlag = "big-load-subscriber-sub-pull-0";
  @Parameter(
      names = {"--message_size"},
      description = "Number of bytes per message."
  )
  public static final Integer messageSizeFlag = 1000;  // 1 KB
  @Parameter(names = {"--publish_batch_size"},
      description = "Number of messages per publish call.")
  public static final Integer publishBatchSizeFlag = 1;
  @Parameter(names = {"--pull_batch_size"},
      description = "Number of messages per pull call.")
  public static final Integer pullBatchSizeFlag = 1;
  @Parameter(names = {"--publish_requests_rate_limit"},
      description = "Maximum rate per second of publish requests.")
  public static final Integer publishRequestsRateLimitFlag = 1;
  @Parameter(names = {"--pull_requests_rate_limit"},
      description = "Maximum rate per second of pull requests.")
  public static final Integer pullRequestsRateLimitFlag = 1;
  @Parameter(names = {"--max_concurrent_publish_request"},
      description = "Maximum number of concurrent publish requests.")
  public static final Integer maxConcurrentPublishRequestsFlag = 1;
  @Parameter(names = {"--max_concurrent_pull_request"},
      description = "Maximum number of concurrent pull requests.")
  public static final Integer maxConcurrentPullRequestsFlag = 1;
  @Parameter(names = {"--seconds_to_run"},
      description = "Number of seconds to run the load test. Use -1 to never stop.")
  public static final Integer secondsToRunFlag = -1;
  @Parameter(names = {"--metrics_report_interval_secs"},
      description = "Number of wait in between reporting the latest metric numbers.")
  public static final Integer metricsReportIntervalSecsFlag = 30;
  @Parameter(names = {"--enable_metrics_report"}, description = "Enable metrics reporting.")
  public static final Boolean enableMetricsReportFlag = true;
  @Parameter(names = {"--request_deadline_millis"}, description = "Request deadline in miliseconds.")
  public static final Integer requestDeadlineMillisFlag = 10000;
  @Parameter(
      names = {"--protocol"},
      description = "Which protocol to talk to Cloud Pub/Sub with.  Either: GRPC"
  )
  public static final Protocol protocolFlag = Protocol.GRPC;
  private static final Logger log = LoggerFactory.getLogger(LoadClient.class);
  private static final int REQUEST_FAILED_CODE = -1;  // A client side error occurred.
  private final int secondsToRun;
  private final String project;
  private final String topicName;
  private final String subscriptionName;
  private final int pullBatchSize;
  private final AccessTokenProvider accessTokenProvider;
  private final RateLimiter publishRateLimiter;
  private final RateLimiter pullRateLimiter;
  private final Semaphore concurrentPublishLimiter;
  private final Semaphore concurrentPullLimiter;
  private final LoadTestStats publishStats;
  private final LoadTestStats pullStats;
  private final ScheduledExecutorService executorService;
  private final MetricsHandler metricsHandler;
  private final int requestDeadlineMillis;
  private final Protocol protocol;
  private final String hostname;
  private final PubsubLoadClientAdapter pubsubClient;
  private final PubsubGrpcLoadClient grpcPubsubClient;
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
    pullBatchSize = builder.pullBatchSize;
    accessTokenProvider = new AccessTokenProvider();
    publishRateLimiter = RateLimiter.create(builder.maxPublishRate);
    pullRateLimiter = RateLimiter.create(builder.maxPullRate);
    concurrentPublishLimiter = new Semaphore(builder.maxConcurrentPublishRequests, false);
    concurrentPullLimiter = new Semaphore(builder.maxConcurrentPullRequests, false);
    requestDeadlineMillis = builder.requestDeadlineMillis;
    protocol = builder.protocol;
    enableMetricReporting = builder.enableMetricsReporting;

    int connections = builder.maxConcurrentPublishRequests + builder.maxConcurrentPullRequests;
    executorService =
        Executors.newScheduledThreadPool(
            connections + 10,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("load-thread").build());
    switch (builder.environment) {
      case PROD:
        hostname = "pubsub.googleapis.com";
        break;
      case STAGING:
        hostname = "staging-pubsub.sandbox.googleapis.com";
        break;
      case QA:
        hostname = "test-pubsub.sandbox.googleapis.com";
        break;
      case EXPERIMENTAL:
        hostname = "pubsub-experimental.googleapis.com";
        break;
      case LOADTEST:
        hostname = "loadtest-pubsub.sandbox.googleapis.com";
        break;
      default:
        throw new RuntimeException("Environment " + builder.environment + " is not recognized.");
    }

    publishStats = new LoadTestStats("publish");
    pullStats = new LoadTestStats("pull");

    metricsHandler =
        new MetricsHandler(project, builder.metricsReportIntervalSecs, accessTokenProvider);
    metricsHandler.initialize();

    ProjectInfo projectInfo = new ProjectInfo(project, topicName, subscriptionName);
    LoadTestParams loadTestParams =
        new LoadTestParams(
            hostname,
            builder.publishBatchSize,
            builder.messageSize,
            pullBatchSize,
            builder.maxConcurrentPublishRequests,
            builder.maxConcurrentPullRequests,
            builder.requestDeadlineMillis);
    switch (protocol) {
      case GRPC:
        grpcPubsubClient =
            new PubsubGrpcLoadClient(
                accessTokenProvider,
                projectInfo,
                loadTestParams);
        pubsubClient = grpcPubsubClient;
        break;
      default:
        throw new IllegalArgumentException("Unreacognized protocol: " + protocol);
    }

    log.info(
        "Load test configured:"
            + "\n\tEnvironment: " + builder.environment
            + "\n\tseconds to run: " + builder.secondsToRun
            + "\n\tproject: " + builder.project
            + "\n\ttopic: " + builder.topic
            + "\n\tsubscription: " + builder.subscription
            + "\n\tmessage size: " + builder.messageSize
            + "\n\trequest deadline milliseconds: " + builder.requestDeadlineMillis
            + "\n\tprotocol: " + builder.protocol
            + "\n\tmax. publish rate: " + builder.maxPublishRate
            + "\n\tmax. concurrent publish requests: " + builder.maxConcurrentPublishRequests
            + "\n\tpublish batch size: " + builder.publishBatchSize
            + "\n\tmax. concurrent pull requests: " + builder.maxConcurrentPullRequests
            + "\n\tpull batch size: " + builder.pullBatchSize);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static void main(String[] args) throws Exception {

    JCommander jCommander = new JCommander(LoadClient.class);
    jCommander.parse(args);
    LoadClient loadClient =
        LoadClient.builder()
            .environment(environmentFlag)
            .secondsToRun(secondsToRunFlag)
            .project(projectFlag)
            .topic(topicFlag)
            .subscription(subscriptionFlag)
            .messageSize(messageSizeFlag)
            .publishBatchSize(publishBatchSizeFlag)
            .pullBatchSize(pullBatchSizeFlag)
            .maxPublishRate(publishRequestsRateLimitFlag)
            .maxPullRate(pullRequestsRateLimitFlag)
            .maxConcurrentPublishRequests(maxConcurrentPublishRequestsFlag)
            .maxConcurrentPullRequests(maxConcurrentPullRequestsFlag)
            .metricsReportIntervalSecs(metricsReportIntervalSecsFlag)
            .requestDeadlineMillis(requestDeadlineMillisFlag)
            .protocol(protocolFlag)
            .enableMetricsReporting(enableMetricsReportFlag)
            .build();

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

    executorService.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            log.info("Printing stats");
            publishStats.printStats();
            pullStats.printStats();
          }
        },
        5,
        10,
        TimeUnit.SECONDS);

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
        for (long latencyMs : result.getEndToEndLatenciesMillis()) {
          metricsHandler.recordEndToEndLatency(latencyMs);
        }
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


  private enum Environment {
    EXPERIMENTAL,
    PROD,
    STAGING,
    QA,
    LOADTEST
  }

  private enum Protocol {
    GRPC
  }

  private enum Operation {
    PUBLISH,
    PULL,
  }

  /**
   * Builder of {@link LoadClient}.
   */
  public static class Builder {
    private Environment environment;
    private int secondsToRun;
    private String project;
    private String topic;
    private String subscription;
    private int messageSize = 1000; // 1KB
    private int publishBatchSize = 1;
    private int pullBatchSize = 1;
    private int maxPublishRate = 1;
    private int maxPullRate = 1;
    private int maxConcurrentPublishRequests = 1;
    private int maxConcurrentPullRequests = 1;
    private int metricsReportIntervalSecs = 1000;  // 1 sec
    private int requestDeadlineMillis = 10000;
    private Protocol protocol = Protocol.GRPC;
    private boolean enableMetricsReporting;

    private Builder() {
    }

    public Builder environment(Environment environment) {
      this.environment = environment;
      return this;
    }

    public Builder secondsToRun(int secondsToRun) {
      this.secondsToRun = secondsToRun;
      return this;
    }

    public Builder project(String project) {
      this.project = project;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder subscription(String subscription) {
      this.subscription = subscription;
      return this;
    }

    public Builder messageSize(int messageSize) {
      this.messageSize = messageSize;
      return this;
    }

    public Builder publishBatchSize(int publishBatchSize) {
      this.publishBatchSize = publishBatchSize;
      return this;
    }

    public Builder pullBatchSize(int pullBatchSize) {
      this.pullBatchSize = pullBatchSize;
      return this;
    }

    public Builder maxPublishRate(int maxPublishRate) {
      this.maxPublishRate = maxPublishRate;
      return this;
    }

    public Builder maxPullRate(int maxPullRate) {
      this.maxPullRate = maxPullRate;
      return this;
    }

    public Builder maxConcurrentPublishRequests(int maxConcurrentPublishRequests) {
      this.maxConcurrentPublishRequests = maxConcurrentPublishRequests;
      return this;
    }

    public Builder maxConcurrentPullRequests(int maxConcurrentPullRequests) {
      this.maxConcurrentPullRequests = maxConcurrentPullRequests;
      return this;
    }

    public Builder metricsReportIntervalSecs(int metricsReportIntervalSecs) {
      this.metricsReportIntervalSecs = metricsReportIntervalSecs;
      return this;
    }

    public Builder requestDeadlineMillis(int requestDeadlineMillis) {
      this.requestDeadlineMillis = requestDeadlineMillis;
      return this;
    }

    public Builder protocol(Protocol protocol) {
      this.protocol = protocol;
      return this;
    }

    public Builder enableMetricsReporting(boolean enableMetricsReporting) {
      this.enableMetricsReporting = enableMetricsReporting;
      return this;
    }

    LoadClient build() throws IOException {
      return new LoadClient(this);
    }
  }
}

