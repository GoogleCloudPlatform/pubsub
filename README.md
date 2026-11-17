[![Build Status](https://travis-ci.org/GoogleCloudPlatform/pubsub.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/pubsub)

This repository contains open-source projects managed by the owners of
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/). The projects
available are:

* [Ordering Keys Prober](https://github.com/GoogleCloudPlatform/pubsub/tree/master/ordering-keys-prober):
  A reference implementation for how to use ordering keys effectively.
* [Flink Connector](https://github.com/GoogleCloudPlatform/pubsub/tree/master/flink-connector):
  Send and receive messages to/from [Apache Flink](https://flink.apache.org/).
* DEPRECATED - [Kafka Connector](https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector):
  Send and receive messages from [Apache Kafka](http://kafka.apache.org). *The
  connector will have future release from its own [repo](https://github.com/googleapis/java-pubsub-group-kafka-connector/).*
* DEPRECATED - [Experimental high-performance client library](https://github.com/GoogleCloudPlatform/pubsub/tree/master/client):
  For Java along with [samples](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-examples/src/main/java/com/google/cloud/examples/pubsub/snippets).

Note: To build each of these projects, we recommend using maven.

The Load Testing Framework that used to live in this repository has been removed
and is no longer supported. If you ran it in the past, check the project you
used for resources it may have left behind and delete any you no longer need:
the `cloud-loadtest-framework-firewall-rule` firewall rule, the
`<project>-cloud-pubsub-loadtest` Cloud Storage bucket, and any `cps-loadtest-*`
instance templates and managed instance groups.
