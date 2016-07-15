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

### Running a Connector

1. Copy the cps-kafka-connector.jar to the place where you will run your Kafka
connector.

2. Create a configuration file for the Cloud Pub/Sub connector and copy it to
the place where you will run Kafka connect. The configuration should set up the
proper Kafka topics, Cloud Pub/Sub topic, and Cloud Pub/Sub project. Sample
configuration files for the source and sink connectors are provided at
configs/.

3. Create an appropriate configuration for your Kafka connect instance. The
only configuration item that must be set specially for the Cloud Pub/Sub
connector to work is the value converter:

`value.converter=com.google.pubsub.kafka.common.ByteStringConverter`

More information on the configuration for Kafka connect can be found in the
[Kafka Users Guide](http://kafka.apache.org/documentation.html#connect_running).

### Important Notes

1. You need a Google Cloud Platform project to use the connector.

2. If you are running the connector on Google Cloud Platform itself, then you need to ensure that
 the machine(s) running the connector have appropriate access to the
Cloud Pub/Sub API's (add instructions on how to do that).

3. If you are not running on Google Cloud Platform, then you will need to create a service account
key that is associated with your project. This key should be provisioned in such a way that it
gives proper access to the Cloud Pub/Sub API's (give instructions on how to do this. The machine
running the connector should have an environment variable, GOOGLE_APPLICATION_CREDENTIALS, which
is set to the path of a file
containing the key.
