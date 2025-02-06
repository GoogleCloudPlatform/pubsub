# Apache Flink Google Cloud Pub/Sub Connector

Note: This connector library is separate from the library maintained by the
Apache Flink community
([https://github.com/apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub)).
The Google Cloud Pub/Sub connector library documentation on
[Apache Flink's website](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/pubsub/)
is **not** for this connector library.

This repository contains a Google Cloud Pub/Sub Flink connector library that is
maintained by the owners of Google Cloud Pub/Sub. This connector was designed to
enable building Flink pipelines that scale to your streaming data analytics
performance requirements. Reasons to use this connector include:

*   Streaming data from Google Cloud Pub/Sub with minimal latency and maximal
    throughput by taking advantage of Google Cloud Pub/Sub's
    [StreamingPull API](https://cloud.google.com/pubsub/docs/pull#streamingpull_api)
*   Processing data at your pace with automatic
    [message lease extensions](https://cloud.google.com/pubsub/docs/lease-management)
*   Compatibility with the latest Flink version (currently
    [Flink 1.19](https://flink.apache.org/2024/03/18/announcing-the-release-of-apache-flink-1.19/))
    and DataStream APIs, including the source API updated in
    [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)

Learn more about this connector in
[`docs/content/docs/connectors/datastream/pubsub.md`](https://github.com/GoogleCloudPlatform/pubsub/blob/master/flink-connector/docs/content/docs/connectors/datastream/pubsub.md),
which discusses it in depth. Still have questions? Please reach out to us by
creating an issue with your question.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream-
and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Using the Connector

We are in the process of uploading the connector to a public repository. In the
meantime, you can build the connector JAR file from source to be packaged with
your Flink deployment.

### Building from Source {#building-from-source}

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
module. The connector library JAR file is
`flink-connector-gcp-pubsub/target/flink-connector-gcp-pubsub-1.0.0-SNAPSHOT.jar`.

Flink applications built with Maven can include the connector as a dependency in
their pom.xml file by adding:

```xml
<dependency>
  <groupId>com.google.pubsub.flink</groupId>
  <artifactId>flink-connector-gcp-pubsub</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Learn more about Flink connector packaging
[here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/).

## Quickstart

This section describes how to build the example Flink job located under
`flink-examples-gcp-pubsub/`, deploy it to a Flink cluster, and verify that it
is running correctly. The example job streams data from a Pub/Sub subscription
source and publishes that data to a Pub/Sub topic sink.

### Prerequisites

1.  Flink cluster

    Flink's
    [first steps guide](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/try-flink/local_installation/)
    describes how to download Flink and start a local-running Flink cluster to
    which you can deploy the example Flink job.

2.  [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
    in which you can create
    [Pub/Sub topics](https://cloud.google.com/pubsub/docs/create-topic) and
    [Pub/Sub subscriptions](https://cloud.google.com/pubsub/docs/create-subscription)

3.  [OPTIONAL] `gcloud` CLI
    ([installation instructions](https://cloud.google.com/sdk/docs/install))

    For your convenience, this section includes `gcloud` commands that you can
    run to complete certain steps. These steps can also be completed using the
    [GCP console UI](https://cloud.google.com/cloud-console) or using one of the
    [Pub/Sub client libraries](https://cloud.google.com/pubsub/docs/reference/libraries).

### Building the Example

Flink jobs are typically packaged into a JAR file with their dependencies. The
following commands use Maven to build and package the example Flink job into a
JAR file that will be deployed to run on a Flink cluster.

```sh
# If you haven't already done so, download this repository.
git clone https://github.com/GoogleCloudPlatform/pubsub.git
cd pubsub/flink-connector

# Run this command in the same directory as this README file.
mvn clean package -DskipTests
```

Running these commands should result in the creation of
`flink-examples-gcp-pubsub/pubsub-streaming/target/PubSubExample.jar`.

### Creating Pub/Sub Topics and Subscriptions

We will use 2 Pub/Sub topics and 2 Pub/Sub subscriptions to run and verify the
example Flink job. The following steps describe how we will use them to validate
data flows through Pub/Sub and the example Flink job as expected.

1.  We manually publish a message to `SOURCE_TOPIC`
2.  Pub/Sub makes the message available for delivery on `SOURCE_SUBSCRIPTION`
    (attached to `SOURCE_TOPIC`)
3.  Flink job pulls the message from `SOURCE_SUBSCRIPTION`
4.  Flink job publishes the message to `SINK_TOPIC`
5.  Pub/Sub makes the message available for delivery on `SINK_SUBSCRIPTION`
    (attached to `SINK_TOPIC`)
6.  We manually pull the message from `SINK_SUBSCRIPTION`, confirming that the
    Flink job successfully used Pub/Sub as both a source and sink

The following commands set variables for your GCP project name, Pub/Sub topic
names, and Pub/Sub subscription names. These variables are referenced in this
section's commands. The placeholder value for `PROJECT_NAME` must be updated to
your GCP project name. The other placeholder values can be used as is or updated
to your liking.

```sh
# This value must be updated to your GCP project name!
export PROJECT_NAME={YOUR-GCP-PROJECT-NAME}

# SOURCE_SUBSCRIPTION is attached to SOURCE_TOPIC
export SOURCE_TOPIC=flink-example-source-topic
export SOURCE_SUBSCRIPTION=flink-example-source-subscription

# SINK_SUBSCRIPTION is attached to SINK_TOPIC
export SINK_TOPIC=flink-example-sink-topic
export SINK_SUBSCRIPTION=flink-example-sink-subscription
```

Create the [topics](https://cloud.google.com/pubsub/docs/create-topic) and
[subscriptions](https://cloud.google.com/pubsub/docs/create-subscription).

```sh
gcloud pubsub topics create $SOURCE_TOPIC --project=$PROJECT_NAME
gcloud pubsub topics create $SINK_TOPIC --project=$PROJECT_NAME
gcloud pubsub subscriptions create $SOURCE_SUBSCRIPTION --topic=$SOURCE_TOPIC --project=$PROJECT_NAME
gcloud pubsub subscriptions create $SINK_SUBSCRIPTION --topic=$SINK_TOPIC --project=$PROJECT_NAME
```

### Accessing Pub/Sub from a Flink Job

1.  Credentials

Requests sent to Google Cloud Pub/Sub must be
[authenticated](https://cloud.google.com/pubsub/docs/authentication). By
default, the connector library authenticates requests using
[Application Default Credentials](https://cloud.google.com/docs/authentication#adc)
(ADC). We recommend setting up ADC in your Flink cluster, if possible, to avoid
having to make changes to the example Flink job.

If your Flink cluster runs on GCP resources (e.g., Google Computer Engine,
Google Kubernetes Engine), ADC is likely already configured to authenticate as a
[service account](https://cloud.google.com/iam/docs/service-account-overview).

To configure ADC in your Flink cluster, follow these
[instructions](https://cloud.google.com/docs/authentication/provide-credentials-adc).
See this
[troubleshooting guide](https://cloud.google.com/docs/authentication/troubleshoot-adc)
if you are having issues with configuring ADC.

If you cannot authenticate using ADC, see this
[section](https://github.com/GoogleCloudPlatform/pubsub/blob/master/flink-connector/docs/content/docs/connectors/datastream/pubsub.md#configuring-access-to-google-cloud-pubsub)
on how to manually set credentials. You will need to make these changes to
`PubSubExample.java` and rebuild the example Flink job JAR file.

1.  Authorization

The authenticated account must have
[permission](https://cloud.google.com/pubsub/docs/access-control) to
[publish messages](https://cloud.google.com/pubsub/docs/access-control#pubsub.publisher)
to `SINK_TOPIC` and
[pull messages](https://cloud.google.com/pubsub/docs/access-control#pubsub.subscriber)
from `SOURCE_SUBSCRIPTION`. These permissions are controlled through IAM
policies, which can be
[set](https://cloud.google.com/pubsub/docs/access-control#setting_a_policy) and
[tested](https://cloud.google.com/pubsub/docs/access-control#testing_permissions)
through the `gcloud` CLI.

### Running and Verifying the Example Flink Job

Start the Flink job.

```sh
# Run this command in the same directory as this README file.
flink run ./flink-examples-gcp-pubsub/pubsub-streaming/target/PubSubExample.jar \
  --project $PROJECT_NAME \
  --source-subscription $SOURCE_SUBSCRIPTION \
  --sink-topic $SINK_TOPIC
```

If successful, the command should output something like: `Job has been submitted
with JobID ...`.

[Publish a message](https://cloud.google.com/pubsub/docs/publisher#publish-messages)
to `SOURCE_TOPIC`.

```sh
gcloud pubsub topics publish projects/$PROJECT_NAME/topics/$SOURCE_TOPIC --message="Hello!"
```

The running Flink job will pull the published message from `SOURCE_SUBSCRIPTION`
and publish it to `SINK_TOPIC`. You can verify this by trying to pull the
message from `SINK_SUBSCRIPTION`.

```sh
# The flag --auto-ack causes pulled messages to be acknowledged, so they will
# not be redelivered if you run this command again.
gcloud pubsub subscriptions pull projects/$PROJECT_NAME/subscriptions/$SINK_SUBSCRIPTION --auto-ack
```

If you are unable to pull the message you published, then follow these steps to
debug your issue and try again after restarting the job.

1.  Use Flink's web UI to help debug the issue. Check the status of the example
    job and cluster logs for errors.
2.  Verify your Pub/Sub topic and subscription configurations. If they are
    configured properly, you should be able to manually publish messages to both
    topics and manually pull messages from both subscriptions. Test this using
    unique message contents so that you can also confirm to which topic each
    subscription is attached.
3.  Run an
    [example job bundled with Flink](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/try-flink/local_installation/#submitting-a-flink-job)
    to test whether your Flink cluster is working correctly.
4.  Ensure that your Flink cluster set up ADC correctly and that the
    authenticating account has permission to pull from `SOURCE_SUBSCRIPTION` and
    publish to `SINK_TOPIC`.

If you are able to pull the message that you published earlier, then you have
successfully ran a Flink job that uses the Pub/Sub connector library!

To learn about using this library in your own Flink job, check out
[`docs/content/docs/connectors/datastream/pubsub.md`](https://github.com/GoogleCloudPlatform/pubsub/blob/master/flink-connector/docs/content/docs/connectors/datastream/pubsub.md).

### Cleaning Up

Delete Pub/Sub [topics](https://cloud.google.com/pubsub/docs/delete-topic) and
[subscriptions](https://cloud.google.com/pubsub/docs/delete-subscriptions).

```sh
gcloud pubsub topics delete $SOURCE_TOPIC --project=$PROJECT_NAME
gcloud pubsub topics delete $SINK_TOPIC --project=$PROJECT_NAME
gcloud pubsub subscriptions delete $SOURCE_SUBSCRIPTION --project=$PROJECT_NAME
gcloud pubsub subscriptions delete $SINK_SUBSCRIPTION --project=$PROJECT_NAME
```

Stop the example Flink job.

```sh
flink cancel {Flink JobID output when it started}
```

## Contributing

Contributions to this library are always welcome and highly encouraged.

See
[CONTRIBUTING](https://github.com/GoogleCloudPlatform/pubsub/blob/master/flink-connector/CONTRIBUTING.md)
for more information on how to get started.

## License

Apache 2.0 - See
[LICENSE](https://github.com/GoogleCloudPlatform/pubsub/blob/master/LICENSE) for
more information.
