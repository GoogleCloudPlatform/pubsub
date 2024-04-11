# Apache Flink Google Cloud Pub/Sub Connector

This repository contains an **unofficial** Flink connector library that is
maintained by the owners of Google Cloud Pub/Sub. This connector was designed to
enable building Flink pipelines that scale to your streaming data analytics
performance requirements. Reasons to use this connector include:

*   Stream data from Google Cloud Pub/Sub with minimal latency and maximal
    throughput by taking advantage of Google Cloud Pub/Sub's
    [StreamingPull API](https://cloud.google.com/pubsub/docs/pull#streamingpull_api)
*   Process data at your pace with automatic
    [message lease extensions](https://cloud.google.com/pubsub/docs/lease-management)
*   Using the latest Flink DataStream APIs, including the source API updated in
    [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)

The official Apache Flink Google Cloud Pub/Sub connector is located at
[https://github.com/apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub).

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream-
and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Using the Connector

We are in the process of uploading the connector to a public respository. In the
meantime, you can build the connector jar file from source to be packaged with
your Flink deployment.

### Building from Source

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

Learn more about Flink connector packaging
[here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/configuration/overview/).

## Documentation

The documentation of Apache Flink is located on the website:
[https://flink.apache.org](https://flink.apache.org) or in the `docs/`
directory.
