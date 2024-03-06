# Google Cloud Pub/Sub Connector

This connector provides access to reading data from and writing data to 
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub).

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

### Google Credentials

By default, Pub/Sub source uses
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
for [authentication](https://cloud.google.com/docs/authentication) to Google
Cloud Pub/Sub. Credentials can be manually set using
`PubSubSource.<String>builder().setCredentials(...)`, which takes precedence
over Application Default Credentials.

The authenticating principal must be
[authorized](https://cloud.google.com/pubsub/docs/access-control) to pull
messages from the subscription. Authorization is managed through Google
[IAM](https://cloud.google.com/security/products/iam) and can be configured on
individual subscriptions.

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
is exceeded. A message is considered outstanding from when Google
Cloud Pub/Sub delivers it until the subscriber acknowledges it. Since
outstanding messages are acknowledged when a checkpoint completes, flow control
limits for outstanding messages are effectively per-checkpoint interval limits.

### Message Leasing

Pub/Sub source automatically extends the acknowledge deadline of messages. This
means a checkpointing interval can be longer than the acknowledge deadline
without causing messages redelivery. Note that acknowledge deadlines can be
extended to at most 1h.

### Performance Considerations

- Infrequent checkpointing can cause performance issues, including subscription
  backlog growth and increased rate of duplicate message delivery.
- `StreamingPull` connections have a 10 MB/s throughput limit. See
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

<!-- TODO(matt-kwong) Add sink details. -->

<!-- TODO(matt-kwong) Add integration testing details. -->
