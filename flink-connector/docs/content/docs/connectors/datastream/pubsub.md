# Google Cloud Pub/Sub Connector

This connector provides access to reading data from and writing data to
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub).

## Usage

This library is currently not published to any repositories. Usage requires
building it from source and packaging it with your Flink application.

Prerequisites:

*   Unix-like environment (we use Linux, Mac OS X)
*   Git
*   Maven (we recommend version 3.8.6)
*   Java 11

```sh
git clone https://github.com/GoogleCloudPlatform/pubsub.git
cd pubsub/flink-connector
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective
module.

Flink applications built with Maven can include the connector as a dependency in
their pom.xml file by adding:

```xml
<dependency>
  <groupId>com.google.pubsub.flink</groupId>
  <artifactId>flink-connector-gcp-pubsub</artifactId>
  <version>0.0.0</version>
</dependency>
```

An example Flink application can be found under
[`flink-connector/flink-examples-gcp-pubsub/`](https://github.com/GoogleCloudPlatform/pubsub/tree/master/flink-connector/flink-examples-gcp-pubsub).

Learn more about Flink connector packaging
[here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/).

## Configuring Access to Google Cloud Pub/Sub

By default, the connector library uses
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
when [authenticating](https://cloud.google.com/docs/authentication) to Google
Cloud Pub/Sub.

Credentials that are set in the source and sink builders take precedence over
Application Default Credentials. Example of setting credentials in builders:

```java
// Set credentials used by Pub/Sub source to pull messages from a subscription.
`PubSubSource.<String>builder().setCredentials(...)`

// Set credentials used by Pub/Sub sink to publish messages to a topic.
PubSubSink.<String>builder().setCredentials(...)
```

The authenticating principal must be
[authorized](https://cloud.google.com/pubsub/docs/access-control) to pull
messages from a subscription when using Pub/Sub source or publish messages to a
topic when using Pub/Sub sink. Authorization is managed through Google
[IAM](https://cloud.google.com/security/products/iam) and can be configured
either at the Google Cloud project-level or the Pub/Sub resource-level.

## Pub/Sub Source

Pub/Sub source streams data from a single Google Cloud Pub/Sub subscription. The
sample below shows the minimal configurations required to build Pub/Sub source.

```java
PubSubSource.<String>builder()
     .setDeserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
     .setProjectName("my-project-name")
     .setSubscriptionName("my-subscription-name")
    .build()
```

### Subscription

Pub/Sub source only supports streaming messages from
[pull subscriptions](https://cloud.google.com/pubsub/docs/pull). Push
subscriptions are not supported.

Pub/Sub source can create parallel readers to to the same subscription. Since
Google Cloud Pub/Sub has no notion of subscription partitions or splits, a
message can be received by any reader. Google Cloud Pub/Sub automatically load
balances message delivery across readers.

### Deserialization Schema

`PubSubDeserializationSchema<T>` is required to define how
[`PubsubMessage`](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
is deserialized into an output that is suitable for processing.

For convenience, `PubSubDeserializationSchema.dataOnly` can be used if only the
field `PubsubMessage.data` is required for processing.

### Boundedness

Pub/Sub source streams unbounded data, only stopping when a Flink job stops or
fails.

### Checkpointing

Checkpointing is required to use Pub/Sub source. When a checkpoint completes,
all messages delivered before the last successful checkpoint are acknowledged to
Google Cloud Pub/Sub. In case of failure, messages delivered after the last
successful checkpoint are unacknowledged and will automatically be redelivered.
Note that there is no message delivery state stored in checkpoints, so retained
checkpoints are not necessary to resume using Pub/Sub source.

### Flow Control

Each Pub/Sub source subtask manages a `StreamingPull` connection to Google Cloud
Pub/Sub. That is, a Pub/Sub source with parallelism of 20 manages 20 separate
`StreamingPull` connections to Google Cloud Pub/Sub. The flow control settings
described in this section are applied to **individual** connections.

Several Pub/Sub source options configure flow control. These options are
illustrated below:

```java
PubSubSource.<OutputT>builder()
    // Allow up to 10,000 message deliveries per checkpoint interval.
    .setMaxOutstandingMessagesCount(10_000L)
    // Allow up to 1000 MB in cumulatitive message size per checkpoint interval.
    .setMaxOutstandingMessagesBytes(1000L * 1024L * 1024L)  // 1000 MB
```

Google Cloud Pub/Sub servers pause message delivery when a flow control setting
is exceeded. A message is considered outstanding from when Google Cloud Pub/Sub
delivers it until the subscriber acknowledges it. Since outstanding messages are
acknowledged when a checkpoint completes, flow control limits for outstanding
messages are effectively per-checkpoint interval limits.

### Message Leasing

Pub/Sub source automatically extends the acknowledge deadline of messages. This
means a checkpointing interval can be longer than the acknowledge deadline
without causing messages redelivery. Note that acknowledge deadlines can be
extended to at most 1h.

### Performance Considerations

-   Infrequent checkpointing can cause performance issues, including
    subscription backlog growth and increased rate of duplicate message
    delivery.
-   `StreamingPull` connections have a 10 MB/s throughput limit. See
    [Pub/Sub quotas and limits](https://cloud.google.com/pubsub/quotas) for all
    restrictions.

<!-- TODO(matt-kwong) Add threading details. -->

### All Options

#### Required Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setSubscriptionName(String subscriptionName)</td>
        <td>(none)</td>
        <td>The ID of the subscription from which Pub/Sub source consumes messages.</td>
    </tr>
    <tr>
        <td>setProjectName(String projectName)</td>
        <td>(none)</td>
        <td>The ID of the GCP project that owns the subscription from which Pub/Sub source consumes messages.</td>
    </tr>
    <tr>
        <td>setDeserializationSchema(PubSubDeserializationSchema&lt;OutputT&gt; deserializationSchema)</td>
        <td>(none)</td>
        <td>How <code>PubsubMessage</code> is deserialized when Pub/Sub source receives a message.</td>
    </tr>
  </tbody>
</table>

#### Optional Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setMaxOutstandingMessagesCount(Long count)</td>
        <td><code>1000L</code></td>
        <td>The maximum number of messages that can be delivered to a Pub/Sub source subtask in a checkpoint interval.</td>
    </tr>
    <tr>
        <td>setMaxOutstandingMessagesBytes(Long bytes)</td>
        <td><code>100L * 1024L * 1024L</code> (100 MB)</td>
        <td>The maximum number of cumulative bytes that can be delivered to a Pub/Sub source subtask in a checkpoint interval.</td>
    </tr>
    <tr>
        <td>setCredentials(Credentials credentials)</td>
        <td>(none)</td>
        <td>The credentials attached to requests sent to Google Cloud Pub/Sub. The identity in the credentials must be authorized to pull messages from the subscription. If not set, then Pub/Sub source uses Application Default Credentials.</td>
    </tr>
  </tbody>
</table>

## Pub/Sub Sink

Pub/Sub sink publishes data to a single Google Cloud Pub/Sub topic. The sample
below shows the minimal configurations required to build Pub/Sub sink.

```java
PubSubSink.<String>builder()
                .setSerializationSchema(
                    PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName("my-project-name")
                .setTopicName("my-topic-name")
                .build()
```

### Serialization Schema

`PubSubSerializationSchema<T>` is required to define how incoming data is
serialized to
[`PubsubMessage`](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage).

For convenience, `PubSubSserializationSchema.dataOnly(SerializationSchema<T>
schema)` can be used if `PubsubMessage.data` is the only field used when
publishing messages.

### Publish Guarantees

There are currently no guarantees that messages are published. Pub/Sub sink uses
a fire-and-forget publishing strategy to maximize throughput, at the cost of
possible data loss.

### All Options

#### Required Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setTopicName(String topicName)</td>
        <td>(none)</td>
        <td>The ID of the topic to which Pub/Sub sink publishes messages.</td>
    </tr>
    <tr>
        <td>setProjectName(String projectName)</td>
        <td>(none)</td>
        <td>The ID of the GCP project that owns the topic to which Pub/Sub sink publishes messages.</td>
    </tr>
    <tr>
        <td>setSerializationSchema(PubSubSerializationSchema&lt;T&gt; serializationSchema)</td>
        <td>(none)</td>
        <td>How incoming data is serialized to <code>PubsubMessage</code>.</td>
    </tr>
  </tbody>
</table>

#### Optional Builder Options

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Builder Method</th>
      <th class="text-left" style="width: 10%">Default Value</th>
      <th class="text-left" style="width: 65%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>setCredentials(Credentials credentials)</td>
        <td>(none)</td>
        <td>The credentials attached to requests sent to Google Cloud Pub/Sub. The identity in the credentials must be authorized to publish messages to the topic. If not set, then Pub/Sub sink uses Application Default Credentials.</td>
    </tr>
  </tbody>
</table>

## Integration Testing

Instead of integration tests reading from and writing to production Google Cloud
Pub/Sub, tests can run against a local instance of the
[Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator). Pub/Sub
source and sink will automatically try connecting to the emulator if the
environment variable `PUBSUB_EMULATOR_HOST` is set.

Steps to run tests against the Pub/Sub emulator:

1.  Ensure that the
    [required dependencies](https://cloud.google.com/pubsub/docs/emulator#before-you-begin)
    and
    [emulator](https://cloud.google.com/pubsub/docs/emulator#install_the_emulator)
    are installed.
2.  Start the emulator using the
    [Google Cloud CLI](https://cloud.google.com/pubsub/docs/emulator#start).
3.  Run the test with the environment variable `PUBSUB_EMULATOR_HOST` set to
    where the emulator is running. For example, if the emulator is listening on
    port 8085 and running on the same machine as the test, set
    `PUBSUB_EMULATOR_HOST=localhost:8085`.
