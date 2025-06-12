# Pub/Sub UDFs

Pub/Sub offers Single Message Transforms ([SMTs](https://cloud.google.com/pubsub/docs/smts/smts-overview)) to simplify data transformations for streaming pipelines. SMTs enable lightweight modifications to message data and attributes directly within Pub/Sub. SMTs eliminate the need for additional data processing steps or separate data transformation products.

A JavaScript User-Defined Function ([UDF](https://cloud.google.com/pubsub/docs/smts/udfs-overview)) is a type of Single Message Transform (SMT). UDFs provide a flexible way to implement custom transformation logic within Pub/Sub


## Using the UDFs

Please clone the repo and use it as a starting point for your Single Message Transforms.


## Deploying the UDFs

All UDFs within this repository are maintained in SQLX format. This format is
used to enable testing and deployment of the UDFs with
the [Dataform CLI tool](https://docs.dataform.co/dataform-cli). \
The Dataform CLI is a useful tool for deploying the UDFs because it:

* Enables unit testing the UDFs
* Automatically identifies dependencies between UDFs and then creates them in
  the correct order.
* Easily deploys the UDFs across different environments (dev, test, prod)

The following sections cover a few methods of deploying the UDFs. 

### Deploy with BigQuery SQL (Fastest)

<details><summary><b>&#128466; Click to expand step-by-step instructions</b></summary>


## Contributing UDFs

![Alt text](/images/public_udf_architecture.png?raw=true "Public UDFs")

If you are interested in contributing UDFs to this repository, please see the
[instructions](/udfs/CONTRIBUTING.md) to get started.