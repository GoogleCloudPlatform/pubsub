This repository contains open-source projects managed by the owners of
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/). The projects
available are:

* [Kafka Connector](https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector):
  Send and receive messages from [Apache Kafka](http://kafka.apache.org).
* [Load Testing Framework](https://github.com/GoogleCloudPlatform/pubsub/tree/master/load-test-framework):
  Set up comparative load tests between [Apache Kafka](http://kafka.apache.org)
  and [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/), as well as
  between different clients on the same stack (e.g. Http/Json and gRPC clients
  for CPS).
* [Experimental high-performance client library](https://github.com/GoogleCloudPlatform/pubsub/tree/master/client):
  For Java along with [samples](https://github.com/GoogleCloudPlatform/pubsub/tree/master/client-samples).

Note: To build each of these projects, we recommend using maven. Currently, we
only support maven version 3 and Java 8. If you're having a problem building
with those versions, please reach out to us with your issue or solution.
