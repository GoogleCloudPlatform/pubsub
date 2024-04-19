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
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

An example Flink application can be found under
[`flink-connector/flink-examples-gcp-pubsub/`](https://github.com/GoogleCloudPlatform/pubsub/tree/master/flink-connector/flink-examples-gcp-pubsub).

Learn more about Flink connector packaging
[here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/).

## Configuring Access to Google Cloud Pub/Sub

Requests sent to Google Cloud Pub/Sub must be
[authenticated](https://cloud.google.com/pubsub/docs/authentication). By
default, the connector library authenticates requests using
[Application Default Credentials](https://cloud.google.com/docs/authentication#adc).

Credentials can also be in the source and sink builders. The connector library
prioritizes using credentials set in builders over Application Default
Credentials. The snippet below shows how to authenticate using an OAuth 2.0
token.

```java
final String tokenValue = "...";

// Authenticate with OAuth 2.0 token when pulling messages from a subscription.
PubSubSource.<String>builder().setCredentials(
    GoogleCredentials.create(new AccessToken(tokenValue, /* expirationTime= */ null)))

// Authenticate with OAuth 2.0 token when publishing messages to a topic.
PubSubSink.<String>builder().setCredentials(
    GoogleCredentials.create(new AccessToken(tokenValue, /* expirationTime= */ null)))
```

The authenticating principal must be
[authorized](https://cloud.google.com/pubsub/docs/access-control) to pull
messages from a subscription when using Pub/Sub source or publish messages to a
topic when using Pub/Sub sink. Authorization is managed through Google
[IAM](https://cloud.google.com/security/products/iam) and can be configured
either at the Google Cloud project-level or the Pub/Sub resource-level.

## Pub/Sub Source

Pub/Sub source streams data from a single Google Cloud Pub/Sub subscription with
an at-least-once guarantee. The sample below shows the minimal configurations
required to build Pub/Sub source.

```java
PubSubSource.<String>builder()
    .setDeserializationSchema(
        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
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

For convenience,
`PubSubDeserializationSchema.dataOnly(DeserializationSchema<T> schema)` can be
used if the field `PubsubMessage.data` stores all of the data to be processed.
Additionally, Flink provides basic schemas for further convenience.

```java
// Deserialization output is PubsubMessage.data field converted to String type.
PubSubSource.<String>builder()
    .setDeserializationSchema(PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
```

Implementing `PubSubDeserializationSchema<T>` is required to process data stored
in fields other than `PubsubMessage.data`.
<!-- TODO(matt-kwong): Add custom deserialization code example. -->

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

### StreamingPull Connections and Flow Control

Each Pub/Sub source subtask opens and manages
[`StreamingPull`](https://cloud.google.com/pubsub/docs/pull#streamingpull_api)
connections to Google Cloud Pub/Sub. The number of connections per subtask can
be set using `PubSubSource.<OutputT>builder().setParallelPullCount` (defaults to
1). Opening more connections can increase the message throughput delivered to
each subtask. Note that the underlying subscriber client library creates an
executor with 5 threads for each connection opened, so too many connections can
be detrimental to performance.

Google Cloud Pub/Sub servers pause message delivery to a `StreamingPull`
connection when a flow control limit is exceeded. There are two forms of flow
control:

1) Message delivery throughput
2) Outstanding message count / bytes

`StreamingPull` connections are limited to pulling messages at 10 MB/s. This
limit cannot be configured. Opening more connections is recommended when
observing message throughput flow control. See
[Pub/Sub quotas and limits](https://cloud.google.com/pubsub/quotas) for a full
list of limitations.

The other form of flow control is based on outstanding messages--when a message
has been delivered but not yet acknowledged. Since outstanding messages are
acknowledged when a checkpoint completes, flow control limits for outstanding
messages are effectively per-checkpoint interval limits. Infrequent
checkpointing can cause connections to be flow controlled due to too many
outstanding messages.

The snippet below shows how to configure connection count and flow control
settings.

```java
PubSubSource.<OutputT>builder()
    // Open 5 StreamingPull connections.
    .setParallelPullCount(5)
    // Allow up to 10,000 message deliveries per checkpoint interval.
    .setMaxOutstandingMessagesCount(10_000L)
    // Allow up to 1000 MB in cumulatitive message size per checkpoint interval.
    .setMaxOutstandingMessagesBytes(1000L * 1024L * 1024L)  // 1000 MB
```

A Pub/Sub source subtask with these options is able to:

- Pull messages at up to 50 MB/s
- Receive up to 50,000 messages **or** 5000 MB in cumulative message size per
  checkpoint interval

### Message Leasing

Pub/Sub source automatically
[extends the acknowledgement deadline](https://cloud.google.com/pubsub/docs/lease-management)
of messages. This means a checkpointing interval can be longer than a message's
acknowledgement deadline without causing message redelivery. Note that message
acknowledgement deadlines are extended for up to 1 hour, after which, they are
redelivered by Google Cloud Pub/Sub.

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
        <td>The maximum number of messages that can be delivered to a StreamingPull connection within a checkpoint interval.</td>
    </tr>
    <tr>
        <td>setMaxOutstandingMessagesBytes(Long bytes)</td>
        <td><code>100L * 1024L * 1024L</code> (100 MB)</td>
        <td>The maximum number of cumulative bytes that can be delivered to a StreamingPull connection within a checkpoint interval.</td>
    </tr>
    <tr>
        <td>setParallelPullCount(Integer parallelPullCount)</td>
        <td>1</td>
        <td>The number of StreamingPull connections to open for pulling messages from Google Cloud Pub/Sub.</td>
    </tr>
    <tr>
        <td>setCredentials(Credentials credentials)</td>
        <td>(none)</td>
        <td>The credentials attached to requests sent to Google Cloud Pub/Sub. The identity in the credentials must be authorized to pull messages from the subscription. If not set, then Pub/Sub source uses Application Default Credentials.</td>
    </tr>
    <tr>
        <td>setEndpoint(String endpoint)</td>
        <td>pubsub.googleapis.com:443</td>
        <td>The Google Cloud Pub/Sub gRPC endpoint from which messages are pulled. Defaults to the global endpoint, which routes requests to the nearest regional endpoint.</td>
    </tr>
  </tbody>
</table>

## Pub/Sub Sink

Pub/Sub sink publishes data to a single Google Cloud Pub/Sub topic with an
at-least-once guarantee. The sample below shows the minimal configurations
required to build Pub/Sub sink.

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

For convenience,
`PubSubSerializationSchema.dataOnly(SerializationSchema<T> schema)` can be used
to write output data to the field `PubsubMessage.data`. The type of
`SerializationSchema<T>` must be one of the supported types matching
[`ByteString.copyFrom(schema.serialize(T ...))`](https://cloud.google.com/java/docs/reference/protobuf/latest/com.google.protobuf.ByteString).


Implementing `PubSubSerializationSchema<T>` is required to publish messages with
attributes or ordering keys.

<!-- TODO(matt-kwong): Add custom serialization code example. -->

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
    <tr>
        <td>setEnableMessageOrdering(Boolean enableMessageOrdering)</td>
        <td>false</td>
        <td>This must be set to true when publishing messages with an ordering key.</td>
    </tr>
    <tr>
        <td>setEndpoint(String endpoint)</td>
        <td>pubsub.googleapis.com:443</td>
        <td>The Google Cloud Pub/Sub gRPC endpoint to which messages are published. Defaults to the global endpoint, which routes requests to the nearest regional endpoint.</td>
    </tr>
  </tbody>
</table>

## Integration Testing

Instead of integration tests reading from and writing to production Google Cloud
Pub/Sub, tests can run against a local instance of the
[Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator). Pub/Sub
source and sink will automatically try connecting to the emulator if the
environment variable `PUBSUB_EMULATOR_HOST` is set. Alternatively, you can
manually set the emulator endpoint in your builder by calling
`.setEndpoint(EmulatorEndpoint.toEmulatorEndpoint("localhost:8085"))`.

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

The emulator can also be started within a Docker container while testing. The
tests under `flink-connector/flink-connector-gcp-pubsub-e2e-tests/` illustrate
how to do this.
