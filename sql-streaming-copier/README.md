# Pub/Sub Copy Pipeline

A dataflow template which performs copies to and from streaming systems using
beam SQL.

This can perform copies from any of:
1) Cloud Pub/Sub
2) Pub/Sub Lite
3) Kafka

This can perform copies to any of:
1) Cloud Pub/Sub
2) Pub/Sub Lite
3) Kafka
4) BigQuery

All sources and sinks should have a simple SQL transform to/from the following
representation, supported natively by Pub/Sub Lite and Kafka:

```sql
CREATE EXTERNAL TABLE <tabletype>(
    message_key BYTES,
    event_timestamp TIMESTAMP,
    attributes ARRAY<ROW<key VARCHAR, values ARRAY<BYTES>>>,
    payload BYTES,
)
```

If copying to Pub/Sub:
* All attribute values must be interpretable as UTF-8
strings. If multiple values are provided for an attribute, they will be joined
with `|` in the Pub/Sub Message.
* The message_key field must be interpretable as UTF-8, and will be stored as
the `beam-sql-copier-message-key` attribute.
  
If copying from Pub/Sub, the `ordering_key` field will be dropped, as it is not
supported by Beam.

If copying to BigQuery, the table must accept the above format if it already
exists.

It requires the following options:

* sourceType: [`pubsub`, `pubsublite`, `kafka`]
* sourceLocation:
  * `pubsub`: The topic (discouraged) or subscription to read from
  * `pubsublite`: The subscription to read from
  * `kafka`: `<host:port>/<topic name>` like `111.128.2.22:8000/my-topic`
* sinkType: [`pubsub`, `pubsublite`, `kafka`, `bigquery`]
* sinkLocation:
  * `pubsub`: The topic to write to
  * `pubsublite`: The topic to write to
  * `kafka`: The broker URL to write to
  * `bigquery`: Location of the table in the BigQuery CLI format

Additional options from
[beam documentation](https://beam.apache.org/documentation/dsls/sql/extensions/create-external-table)
can be provided using the `sourceOptions` and `sinkOptions` parameters.

## Example run command

```bash
gcloud dataflow flex-template run "streaming-copier-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://pubsub-streaming-sql-copier/template/copier.json" \
    --region "us-central1" \
    --parameters sourceType=pubsub \
    --parameters sourceLocation="projects/pubsub-public-data/topics/taxirides-realtime" \
    --parameters sinkType=pubsublite \
    --parameters sinkLocation="projects/<my project>/locations/us-central1-a/topics/taxirides-realtime-clone"
```

## How to build

### Shaded jar generation

To regenerate the shaded jar, clone the beam repo locally and put the following
into a `custom-shadowjar/build.gradle` package in the beam source tree. Add
`include(":custom-shadowjar")` to `settings.gradle.kts` and run
`./gradlew :custom-shadowjar:shadowJar`.

```groovy
plugins {
    id 'java'
    id 'org.apache.beam.module'
    id 'com.github.johnrengelman.shadow'
}
applyJavaNature(
        automaticModuleName: 'org.apache.beam.sql-shaded'
)

dependencies {
    implementation project(":sdks:java:extensions:sql")
    implementation project(":sdks:java:extensions:sql:zetasql")
    implementation project(":sdks:java:io:google-cloud-platform")
    implementation project(":runners:direct-java")
    implementation project(":runners:google-cloud-dataflow-java")
    implementation project(":sdks:java:io:kafka")
}

shadowJar {
    mergeServiceFiles()
    zip64 true
}
```

Then, run the following command which will properly set up the third_party
maven repo:

```bash
export BUILT_JAR_PATH="<path to beam git clone>/custom-shadowjar/target/<jarname>"
export THIRD_PARTY_PATH="<path to this cloned git repo>/sql-streaming-copier/third_party"

mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
-Dfile="$BUILT_JAR_PATH" \
-DgroupId=com.google.cloud.pubsub \
-DartifactId=beam-custom-shadowjar \
-Dversion=1.0.0 \
-Dpackaging=jar \
-DlocalRepositoryPath="$THIRD_PARTY_PATH"
```

### Deploying

First, from the `sql-streaming-copier` directory, run `mvn package`.

Then, run the following to upload the new version of the template.

```bash
export TEMPLATE_PATH="gs://<TEMPLATE UPLOAD LOCATION>.json"
export TEMPLATE_IMAGE="gcr.io/<PROJECT>/<CONTAINER PATH>:latest"

gcloud dataflow flex-template build $TEMPLATE_PATH \
    --image-gcr-path "$TEMPLATE_IMAGE" \
    --sdk-language "JAVA" \
    --flex-template-base-image JAVA11 \
    --metadata-file "metadata.json" \
    --jar "target/sql-streaming-copier-1.0.0.jar" \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.pubsub.sql.TemplateMain"
```
