A beam template which performs copies to and from streaming systems using beam SQL.

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

If copying to BigQuery, the table must accept the above format if it already
exists.

It requires the following options:

* sourceType: [`pubsub`, `pubsublite`, `kafka`]
* sourceLocation:
  * `pubsub`: The topic (discouraged) or subscription to read from
  * `pubsublite`: The subscription to read from
  * `kafka`: The topic URL to read from
* sinkType: [`pubsub`, `pubsublite`, `kafka`, `bigquery`]
* sinkLocation:
  * `pubsub`: The topic to write to
  * `pubsublite`: The topic to write to
  * `kafka`: The topic URL to write to
  * `bigquery`: Location of the table in the BigQuery CLI format


Additional options from
[beam documentation](https://beam.apache.org/documentation/dsls/sql/extensions/create-external-table)
can be provided using the `sourceOptions` and `sinkOptions` parameters.

-------- BUILD NOTES --------------

To regenerate the shaded jar, put the following into a `custom-shadowjar`
package in the beam source tree and run `./gradlew :custom-shadowjar:shadowJar`

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
}

shadowJar {
    zip64 true
}
```
