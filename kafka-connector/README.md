### Introduction

The CloudPubSubConnector is a connector to be used with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) to publish messages from
[Kafka](http://kafka.apache.org) to [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/) and vice versa. CloudPubSubConnector provides
both a sink connector (to copy messages from Kafka to Cloud Pub/Sub) and a
source connector (to copy messages from Cloud Pub/Sub to Kafka).

### Building

These instructions assume you are using [Maven](https://maven.apache.org/).

1.  If you want to build the connector from head, clone the repository, ensuring
    to do so recursively to pick up submodules:

    `git clone --recursive https://github.com/GoogleCloudPlatform/pubsub`

    If you wish to build from a released version of the connector, download it
    from the [Releases section](https://github.com/GoogleCloudPlatform/pubsub/releases)
    in GitHub.

2.  Unzip the source code if downloaded from the release version.

3.  Go into the kafka-connector directory in the cloned repo or downloaded
    release.

4.  Make the jar that contains the connector:

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

3.  Create an appropriate configuration for your Kafka connect instance. More
    information on the configuration for Kafka connect can be found in the
    [Kafka Users Guide](http://kafka.apache.org/documentation.html#connect_running).
    
4.  If running the Kafka Connector behind a proxy, you need to export the
    KAFKA_OPTS variable with options for connecting around the proxy. You can
    export this variable as part of a shell script in order ot make it easier.
    Here is an example:
 
   `export KAFKA_OPTS="-Dhttp.proxyHost=<host> -Dhttp.proxyPort=<port> -Dhttps.proxyHost=<host> -Dhttps.proxyPort=<port>"`

### CloudPubSubConnector Configs

In addition to the configs supplied by the Kafka Connect API, the Pubsub
Connector supports the following configs:

#### Source Connector

| Config | Value Range | Default | Description |
|------------------------|-----------------------------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cps.subscription | String | REQUIRED (No default) | The name of the subscription to Cloud Pub/Sub, e.g. "sub" for topic "/projects/bar/subscriptions/sub". |
| cps.project | String | REQUIRED (No default) | The project containing the topic from which to pull messages, e.g. "bar" from above. |
| kafka.topic | String | REQUIRED (No default) | The topic in Kafka which will receive messages that were pulled from Cloud Pub/Sub. |
| cps.maxBatchSize | Integer | 100 | The minimum number of messages to batch per pull request to Cloud Pub/Sub. |
| kafka.key.attribute | String | null | The Cloud Pub/Sub message attribute to use as a key for messages published to Kafka. |
| kafka.partition.count | Integer | 1 | The number of Kafka partitions for the Kafka topic in which messages will be published to. NOTE: this parameter is ignored if partition scheme is "kafka_partitioner".|
| kafka.partition.scheme | round_robin, hash_key, hash_value, kafka_partitioner | round_robin | The scheme for assigning a message to a partition in Kafka. The scheme "round_robin" assigns partitions in a round robin fashion, while the schemes "hash_key" and "hash_value" find the partition by hashing the message key and message value respectively. "kafka_partitioner" scheme delegates partitioning logic to kafka producer, which by default detects number of partitions automatically and performs either murmur hash based partition mapping or round robin depending on whether message key is provided or not.|
| gcp.credentials.file.path | String | Optional | The file path, which stores GCP credentials.| If not defined, GOOGLE_APPLICATION_CREDENTIALS env is used. |
| gcp.credentials.json | String | Optional | GCP credentials JSON blob | If specified, use the explicitly handed credentials. Consider using the externalized secrets feature in Kafka Connect for passing the value. |
| kafka.record.headers | Boolean | false | Use Kafka record headers to store Pub/Sub message attributes |

#### Sink Connector

| Config | Value Range | Default | Description |
|---------------|-------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------|
| cps.topic | String | REQUIRED (No default) | The topic in Cloud Pub/Sub to publish to, e.g. "foo" for topic "/projects/bar/topics/foo". |
| cps.project | String | REQUIRED (No default) | The project in Cloud Pub/Sub containing the topic, e.g. "bar" from above. |
| maxBufferSize | Integer | 100 | The maximum number of messages that can be received for the messages on a topic partition before publishing them to Cloud Pub/Sub. |
| maxBufferBytes | Long | 10000000 | The maximum number of bytes that can be received for the messages on a topic partition before publishing them to Cloud Pub/Sub. |
| maxDelayThresholdMs | Integer | 100 | The maximum amount of time to wait to reach maxBufferSize or maxBufferBytes before publishing outstanding messages to Cloud Pub/Sub. |
| maxRequestTimeoutMs | Integer | 10000 | The timeout for individual publish requests to Cloud Pub/Sub. |
| maxTotalTimeoutMs | Integer | 60000| The total timeout for a call to publish (including retries) to Cloud Pub/Sub. |
| gcp.credentials.file.path | String | Optional | The file path, which stores GCP credentials.| If not defined, GOOGLE_APPLICATION_CREDENTIALS env is used. |
| gcp.credentials.json | String | Optional | GCP credentials JSON blob | If specified, use the explicitly handed credentials. Consider using the externalized secrets feature in Kafka Connect for passing the value. |
| metadata.publish | Boolean | false | When true, include the Kafka topic, partition, offset, and timestamp as message attributes when a message is published to Cloud Pub/Sub. |
| headers.publish | Boolean | false | When true, include any headers as attributes when a message is published to Cloud Pub/Sub. |

#### Schema Support and Data Model

A pubsub message has two main parts: the message body and attributes. The
message body is a [ByteString](https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/ByteString)
object that translates well to and from the byte[]
bodies of Kafka messages. For this reason, we recommend using a converter that
produces primitive data types (i.e. integer, float, string, or bytes schema)
where possible to prevent deserializing and reserializing the same message body.

Additionally, a Pubsub message size cannot exceed 10MB, so please check
your broker's message.max.bytes configuration to prevent possible errors.

The sink connector handles the conversion in the following way:

*   For integer, float, string, and bytes schemas, the bytes of the Kafka
    message's value are passed directly into the Pubsub message body.
*   For map and struct types, the values are stored in attributes. Pubsub only
    supports string to string mapping in attributes. To make the connector as
    versatile as possible, the toString() method will be called on whatever
    object passed in as the key or value for a map and the value for a struct.
    *   One additional feature is we allow a specification of a particular
        field or key to be placed in the Pubsub message body. To do so, set the
        messageBodyName configuration with the struct field or map key.
        This value is stored as a ByteString, and any integer, byte, float, or
        array type is included in the message body as if it were the sole value.
*   For arrays, we only support primitive array types due to potential
    collisions of field names or keys of a struct/map array. The connector
    handles arrays in a fairly predictable fashion, each value is concatenated
    together into a ByteString object.
*   In all cases, the Kafka key value is stored in the Pubsub message's
    attributes as a string, currently "key".
    
> **IMPORTANT NOTICE:** There are three limitations to keep in mind when using Pubsub
message attributes as stated on its [documentation](https://cloud.google.com/pubsub/quotas#resource_limits)
>* *"Attributes per message: 100"*
>* *"Attribute key size: 256 bytes"*
>* *"Attribute value size: 1024 bytes"*
>
>If you enable copy of Kafka headers as Pubsub message attribute (it is disabled by default), the connector will copy 
>only those headers meeting these limitations and will skip those that do not.


The source connector takes a similar approach in handling the conversion
from a Pubsub message into a SourceRecord with a relevant Schema.

*   The connector searches for the given kafka.key.attribute in the
    attributes of the Pubsub message. If found, this will be used as the Kafka
    key with a string schema type. Otherwise, it will be set to null.
*   If the Pubsub message doesn't have any other attributes, the message body
    is stored as a byte[] for the Kafka message's value.
*   However, if there are attributes beyond the Kafka key, the value is assigned
    a struct schema. Each key in the Pubsub message's attributes map becomes a
    field name, with the values set accordingly with string schemas. In this
    case, the Pubsub message body is identified by the field name set in
    "message", and has the schema types bytes.
    *   In these cases, to carry forward the structure of data stored in
        attributes, we recommend using a converter that can represent a struct
        schema type in a useful way, e.g. JsonConverter.
 
