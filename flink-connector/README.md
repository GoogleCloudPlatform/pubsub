# Apache Flink Google Cloud Pub/Sub Connector (Under Development)

The official Apache Flink Google Cloud Pub/Sub connector is located at
[https://github.com/apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub).

This repository contains an **unofficial** connector that is **under
development** by the owners of Google Cloud Pub/Sub. The connector is available
to preview, but there are currently no performance or stability guarantees.

Some motivations behind creating this connector:

*   Create a Google Cloud Pub/Sub source that implements the Source interface
    introduced in
    [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
*   Leverage
    [StreamingPull API](https://cloud.google.com/pubsub/docs/pull#streamingpull_api)
    to achieve high throughput and low latency in this connector source
*   Add support for automatic
    [message lease extensions](https://cloud.google.com/pubsub/docs/lease-management)
    to enable setting longer checkpointing intervals

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream-
and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Building from Source

Prerequisites:

*   Unix-like environment (we use Linux, Mac OS X)
*   Git
*   Maven (we recommend version 3.8.6)
*   Java 11

```
git clone https://github.com/GoogleCloudPlatform/pubsub.git
cd pubsub/flink-connector
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective
module.

## Documentation

The documentation of Apache Flink is located on the website:
[https://flink.apache.org](https://flink.apache.org) or in the `docs/`
directory.
