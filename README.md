### Introduction

The CloudPubSubConnector is a connector to be used with
[Kafka Connect](http://kafka.apache.org/documentation.html#connect) to publish
messages from [Kafka](http://kafka.apache.org) to
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) and vice versa. CloudPubSubConnector
provides both a sink connector (to copy messages from Kafka to Cloud Pub/Sub)
and a source connector (to copy messages from Cloud Pub/Sub to Kafka).

Since Cloud Pub/Sub has no notion of partition or key, the sink connector stores
these as attributes on a message.

### Limitations

Cloud Pub/Sub does not allow any message to be larger than 10MB. Therefore, if
your Kafka topic has messages that are larger than this, the connector cannot
be used. Consider setting message.max.bytes on your broker to ensure that no
messages will be larger than that limit.

### Building

These instructions assumes you are using [Maven](https://maven.apache.org/).

1. Clone the repository, ensuring to do so recursively to pick up submodules:

 `git clone --recursive ADD LINK ONCE REPO LOCATION IS DETERMINED`

2. Make the jar that contains the connector:

 `mvn package`

The resulting jar is at target/cps-kafka-connector.jar.

### Running a Sink Connector

1. Copy the cps-kafka-connector.jar to the place where you will run your Kafka
connnector.

2. Create a configuration file for the Cloud Pub/Sub connector and copy it to
the place where you will run Kafka connect. The configuraiton should set up the
proper Kafka topics, Cloud Pub/Sub topic, and Cloud Pub/Sub project. A sample
configuration file is provided at configs/cps-sink-connector.properites.

3. Create an appropriate configuration for your Kafka connect instance. The
only configuration item that must be set specially for the Cloud Pub/Sub
connector to work is the value converter:

`value.converter=com.google.pubsub.kafka.common.ByteStringConverter`

More information on the configuration for Kafka connect can be found in the
[Kafka Users Guide](http://kafka.apache.org/documentation.html#connect_running).
