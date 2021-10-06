The Republisher is an experimental horizontally scaling job to be run on GKE. It
accepts incoming publishes from various frameworks and republishes to either
Cloud Pub/Sub or Pub/Sub Lite.

## Routing

The target system to publish messages to is determined by the structure of the
topic path sent over the wire protocol. In general, paths structured like:

`projects/my-project/topics/my-topic`

Will be sent to Cloud Pub/Sub, while paths structured like:

`projects/my-project/locations/xxxxxxxxx/topics/my-topic`

Will be sent to Pub/Sub Lite.

## Scaling

The Republisher instance is a single machine deployment of many protocol
services. There is no communication required between instances, and the
instances themselves are largely stateless. 

## Protocols

The Republisher accepts messages using the following wire protocols
in the following way:

### MQTT

MQTT handles the MQTT wire protocol using
[HiveMQ community edition](https://github.com/hivemq/hivemq-community-edition)
which is an extensible Apache Licensed MQTT implementation. When it receives a
PUBLISH request, it will not respond with a PUBACK until the message is
acknowledged by the Pub/Sub system. Messages will not be stored to local disk.
SUBSCRIBE requests will be ignored.

MQTT accepts publishes on port `1883` by default. All
[HiveMQ configuration settings](https://www.hivemq.com/docs/hivemq/3.4/user-guide/configuration.html#configuration-files)
can be configured by setting the `HIVE_MQ_CONFIG_FOLDER` environment variable to
a folder on your container with a file named `config.xml` in it.

## Building

To build the dockerfile, run the following script from the `republisher`
directory:

```bash
mvn package
docker build .
```
