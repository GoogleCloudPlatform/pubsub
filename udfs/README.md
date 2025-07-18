# Pub/Sub UDFs

Pub/Sub offers Single Message Transforms ([SMTs](https://cloud.google.com/pubsub/docs/smts/smts-overview)) to simplify data transformations for streaming pipelines. SMTs enable lightweight modifications to message data and attributes directly within Pub/Sub. SMTs eliminate the need for additional data processing steps or separate data transformation products.

A JavaScript User-Defined Function ([UDF](https://cloud.google.com/pubsub/docs/smts/udfs-overview)) is a type of Single Message Transform (SMT). UDFs provide a flexible way to implement custom transformation logic within Pub/Sub


## Using the UDFs

Please clone the repo and use it as a starting point for your Single Message Transforms. All UDFs within this repository are maintained in js format. This format is used to enable testing and deployment of the UDFs with Pub/Sub SMTs.