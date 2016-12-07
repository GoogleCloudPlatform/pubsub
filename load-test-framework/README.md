### Introduction

This load test framework, known as Flic (Framework of load & integration for
Cloud Pub/Sub), for Cloud Pub/Sub is a tool targeted for developers and
companies who wish to benchmark Cloud Pub/Sub and Kafka.

The goal of this framework is twofold:

1.  Provide users with a tool that allows them to see how Cloud Pub/Sub performs
    under various conditions.

2.  Provide users with a tool that allows them to benchmark their own Kafka
    configuration.

### Quickstart

You must have [Maven](https://maven.apache.org/) installed, be running on an Unix environment, either Linux or Mac OS X, and have the `zip` command line utility available. You can then run `python run.py --project=<your_project>` which will install the load test framework and run a basic load test.

### Pre-Running Steps

1.  Regardless of whether you are running on Google Cloud Platform or not, you
    need to create a project and create a service key that allows you access to
    the Google Cloud Pub/Sub, Storage, and Monitoring APIs.

2.  Create project on Google Cloud Platform. By default, this project will have
    multiple service accounts associated with it (see "IAM & Admin" within GCP
    console). Within this section, find the tab for "Service Accounts". Create a
    new service account and make sure to select "Furnish a new private key".
    Doing this will create the service account and download a private key file
    to your local machine.

3.  Go to the "IAM" tab, find the service account you just created and click on
    the dropdown menu named "Role(s)". Under the "Pub/Sub" submenu, select
    "Pub/Sub Admin".

    If you don't see the service account in the list, add a new permission, use
    the service account as the member name, and select "Pub/Sub Admin" from the
    role dropdown menu in the window.

    Now, the service account you just created should appear in the members list
    on the IAM page with the role Pub/Sub Admin. If the member name is gray,
    don't worry. It will take a few minutes for the account's new permissions to
    make their way through the system.

    Finally, the key file that was downloaded to your machine
    needs to be placed on the machine running the framework. An environment
    variable named GOOGLE_APPLICATION_CREDENTIALS must point to this file. (Tip:
    export this environment variable as part of your shell startup file).

    `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key/file`

### Important Notes

There are important differences in the clients used for Cloud Pub/Sub and Kafka
in this framework. For Cloud Pub/Sub, we implemented a reasonably optimized
client with parallelization of requests across multiple threads, asynchronous
callback threads to increase client throughput, batching, and rate limitation.
For Kafka, we used the Producer API released by Apache, which includes some of
those same features or rate limits, batching, and asynchronous callback threads,
but of course they're implemented slightly differently to better integrate with
Kafka brokers.

We took care to make sure the data recording was as similar as possible. Both clients
measure total latency as the time a message is added to a batch to be sent until its
callback method is called. Although exactly what load the service sees might differ
slightly, the record of how it responds, its latency and throughput, are equivalent.
