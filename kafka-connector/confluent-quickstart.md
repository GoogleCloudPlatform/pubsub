### Prerequisite: 
You have completed confluent quick-start steps from here: 
https://docs.confluent.io/platform/current/quickstart/ce-quickstart.html#ce-quickstart


### Steps to Follow: 
1.  Create a folder called jar. For example say at /path/to/jar 
2.  Place pubsub-kafka-connector.jar inside /path/to/jar
3.  Create a file <properties-file-location>/connect_config.properties, add this content to the file after making appropriate edits: 

``` properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
bootstrap.servers=<server>:<port> #default is localhost:9092
plugin.path=/path/to/jar #Edit to actual path
offset.storage.file.filename=<preferred-folder>/temp/connect.offset
rest.port=100 # Change this as needed
# Optional- point to your gcp credential file
gcp.credentials.file.path=/<path-to-credentials-file>/<credential-file-name>.json #Edit to actual path
```

4.  Create a sink configurations file, and let’s call it <properties-file-location>/sink.properties and add this text and make appropriate edits: 

``` properties
name=CPSSinkConnector
connector.class=com.google.pubsub.kafka.sink.CloudPubSubSinkConnector
tasks.max=10
topics=<source-kafka-topic> # if using confluent datagen from the quickstart guide, this value will be either users or pageviews
cps.topic=<destination-pubsub-topic>
cps.project=<destination-google-project>
```

5. Run it: 

 ``` bash
/path-to-confluent/bin/connect-standalone properties-file-location/connect_config.properties/connect_config.properties properties-file-location/sink.properties



