### Introduction

The CloudPubSubConnector is a connector to be used with [Kafka Connect]
(http://kafka.apache.org/documentation.html#connect) to publish messages from
[Kafka](http://kafka.apache.org) to [Google Cloud Pub/Sub]
(https://cloud.google.com/pubsub/) and vice versa. CloudPubSubConnector provides
both a sink connector (to copy messages from Kafka to Cloud Pub/Sub) and a
source connector (to copy messages from Cloud Pub/Sub to Kafka).

Since Cloud Pub/Sub has no notion of partition or key, the sink connector stores
these as attributes on a message.

### Limitations

Cloud Pub/Sub does not allow any message to be larger than 10MB. Therefore, if
your Kafka topic has messages that are larger than this, the connector cannot be
used. Consider setting message.max.bytes on your broker to ensure that no
messages will be larger than that limit.

### Building

These instructions assume you are using [Maven](https://maven.apache.org/).

1.  Clone the repository, ensuring to do so recursively to pick up submodules:

    `git clone --recursive ADD LINK ONCE REPO LOCATION IS DETERMINED`

2.  Make the jar that contains the connector:

    `mvn package`

The resulting jar is at target/cps-kafka-connector.jar.

### Pre-Running Steps

1.  Regardless of whether you are running on Google Cloud Platform or not, you
    need to create a project and create service key that allows you access to
    the Cloud Pub/Sub API's and default quotas.

2.  Create project on Google Cloud Platform. By default, this project will have
    multiple service accounts associated with it (see "IAM & Admin" within GCP
    console). Within this section, find the tab for "Service Accounts". Create a
    new service account and make sure to select "Furnish a new private key".
    Doing this will create the service account and download a private key file
    to your local machine.

3.  Go to the "IAM" tab, find the service account you just created and click on
    the dropdown menu named "Role(s)". Under the "Pub/Sub" submenu, select
    "Pub/Sub Admin". Finally, the key file that was downloaded to your machine
    needs to be placed on the machine running the framework. An environment
    variable named GOOGLE_APPLICATION_CREDENTIALS must point to this file. (Tip:
    export this environment variable as part of your shell startup file).

    `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/file`

### Running a Connector

1.  Copy the cps-kafka-connector.jar to the place where you will run your Kafka
    connector.

2.  Create a configuration file for the Cloud Pub/Sub connector and copy it to
    the place where you will run Kafka connect. The configuration should set up
    the proper Kafka topics, Cloud Pub/Sub topic, and Cloud Pub/Sub project.
    Sample configuration files for the source and sink connectors are provided
    at configs/.

3.  Create an appropriate configuration for your Kafka connect instance. The
    only configuration item that must be set specially for the Cloud Pub/Sub
    connector to work is the value converter:

    `value.converter=com.google.pubsub.kafka.common.ByteStringConverter`

If the messages you plan on publishing to Kafka also contain keys, then make
sure that the key.converter is a class that will be able to work with the schema
of your keys. More information on the configuration for Kafka connect can be
found in the [Kafka Users Guide]
(http://kafka.apache.org/documentation.html#connect_running).

### CloudPubSubConnector Configs

Cloud Pub/Sub topics, and subscriptions are represented by their
fully qualified path name. For example a topic "foo" that lives under
the project "bar" will have a topic name of "projects/bar/topics/foo". 
When specifying configs for the connector, do not include the fully
qualified path name that you see on Cloud Pub/Sub. 
Rather, just include the single-word name (i.e "foo" in this case).


#### Sink Connector

| Config           | Value Range         | Default                      | Description                                                                                                                        | 
|------------------|---------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| cps.topic        | String              | REQUIRED (No default)        | The topic to which to publish.                                                                                                     |
| cps.project      | String              | REQUIRED (No default)        | The project containing the topic to which to publish.                                                                              |
| maxBufferSize    | Integer             | 100                          | The maximum number of messages that can be received for the messages on a topic partition before publishing them to Cloud Pub/Sub. |


#### Source Connector

| Config                 | Value Range   | Default               | Description
|------------------------|---------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cps.subscription       | String                                | REQUIRED (No default) | The name of the subscription to Cloud Pub/Sub.                                                                                                                                                                                                                |
| cps.project            | String                                | REQUIRED (No default) | The project containing the topic from which to pull messages.                                                                                                                                                                                                 |
| kafka.topic            | String                                | REQUIRED (No default) | The topic in Kafka which will receive messages that were pulled from Cloud Pub/Sub.                                                                                                                                                                           |
| cps.maxBatchSize       | Integer                               | 100                   | The minimum number of messages to batch per pull request to Cloud Pub/Sub.                                                                                                                                                                                    |
| kafka.key.attribute    | String                                | null                  | The Cloud Pub/Sub message attribute to use as a key for messages published to Kafka.                                                                                                                                                                          |
| kafka.partition.count  | Integer                               | 1                     | The number of Kafka partitions for the Kafka topic in which messages will be published to.                                                                                                                                                                    |
| kafka.partition.scheme | round_robin, hash_key, hash_value     | round_robin           | The scheme for assigning a message to a partition in Kafka. The scheme "round_robin" assigns partitions in a round robin fashion, while the schemes "hash_key" and "hash_value" find the partition by hashing the message key and message value respectively. |
