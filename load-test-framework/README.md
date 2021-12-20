### Introduction

This load test framework, known as Flic (Framework of load & integration for
Cloud Pub/Sub), for Cloud Pub/Sub is a tool targeted for developers and
companies who wish to benchmark Cloud Pub/Sub.

The goal of this framework is to provide users with a tool that allows them to see how Cloud Pub/Sub
performs under various conditions.

#### IMPORTANT NOTE:

Running this loadtest framework in throughput testParameters on a 16 core machine can consume somewhere between 1 and 2 TB of
pubsub data usage, which will cost from 60 to 120 dollars.  Please take this into account before running this loadtest.

### Quickstart

You must have [Maven](https://maven.apache.org/) installed, be running on an Unix environment, either Linux or Mac OS X,
and have the `zip` command line utility available.
You can then run `python run.py --project=<your_project>` which will install the load test framework and run a basic
load test.

The `--test_parameters` parameter changes the test type. The default is 'latency', but it can also be set to
'throughput' which will test the maximum throughput on a single VM at different numbers of cores (defaulting to 16).

The `--language` parameter sets what language should be tested, which can be one of `JAVA`, `GO`, `PYTHON` or `NODE`.

Many parameters made for the loadtest can be overridden using flags.  Run the loadtest with only the `--help` flag set
to see all available options.  Note that RUBY and DOTNET clients have not yet been implemented.

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
    the dropdown menu named "Role(s)". Select "Project Editor". The load test
    framework requires permissive access since it creates GCE templates,
    creates Pub/Sub topics, and modifies firewalls. If you are uncomfortable
    with the permissions, please use a new GCP project for running load tests.

    If you don't see the service account in the list, add a new permission, use
    the service account as the member name, and select "Project Editor" from the
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
