#!/usr/bin/env python3

"""Pub/Sub <-> Kafka Simple Copy Tool

A python script for downloading, installing and running the Kafka connector in
a single machine configuration. More complex set-ups should look at the kafka
connect documentation
(https://docs.confluent.io/home/connect/userguide.html).
"""

import argparse
import io
import os
import platform
import requests
import tarfile
import tempfile
import subprocess

KAFKA_RELEASE = "2.6.0"
KAFKA_FOLDER = f"kafka_2.13-{KAFKA_RELEASE}"
KAFKA_LINK = f"https://archive.apache.org/dist/kafka/{KAFKA_RELEASE}/{KAFKA_FOLDER}.tgz"

CONNECTOR_RELEASE = "v0.10-alpha"
PUBSUB_CONNECTOR_LINK = f"https://github.com/GoogleCloudPlatform/pubsub/releases/download/{CONNECTOR_RELEASE}/pubsub-kafka-connector.jar"


def extract_kafka_to(tempdir):
    response = requests.get(KAFKA_LINK)
    with tarfile.open(fileobj=io.BytesIO(response.content), mode='r:gz') as archive:
        archive.extractall(path=tempdir)


def get_connector(tempdir):
    response = requests.get(PUBSUB_CONNECTOR_LINK)
    with open(os.path.join(tempdir, "pubsub-kafka-connector.jar"), 'wb+') as f:
        f.write(response.content)


def download(tempdir):
    print("Downloading kafka...")
    kafka_path = os.path.join(tempdir, "kafka")
    os.mkdir(kafka_path)
    extract_kafka_to(kafka_path)
    print("Downloading connector...")
    connector_path = os.path.join(tempdir, "connector")
    os.mkdir(connector_path)
    get_connector(connector_path)


def make_connect_config(tempdir, bootstrap_servers):
    print("Building connect config...")
    with open(os.path.join(tempdir, "connect_config.properties"),
              'w+') as output:
        output.write(
            "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n")
        output.write(
            "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter\n")
        output.write(f"bootstrap.servers={bootstrap_servers}\n")
        connector_dir = os.path.join(tempdir, "connector")
        output.write(f"plugin.path={connector_dir}\n")
        offset_file = os.path.join(tempdir, "connect.offset")
        output.write(f"offset.storage.file.filename={offset_file}\n")


def run_connector(tempdir, connector_properties):
    if platform.system() == "Windows":
        kafka_script = os.path.join(tempdir, "kafka", KAFKA_FOLDER, "bin",
                                    "windows", "connect-standalone.bat")
    else:
        kafka_script = os.path.join(tempdir, "kafka", KAFKA_FOLDER, "bin",
                                    "connect-standalone.sh")
    connect_properties = os.path.join(tempdir, "connect_config.properties")
    print("Running")
    subprocess.run(
        args=[kafka_script, connect_properties, connector_properties],
        cwd=tempdir)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap_servers",
        type=str,
        required=True,
        help="A comma-separated list of kafka servers to use for bootstrapping."
    )
    parser.add_argument(
        "--connector_properties_file",
        type=str,
        required=True,
        help="""The Pub/Sub connector configuration file.
        
        `connector.class` specifies which connector to use.
        Other arguments to this configuration may be found at
        https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector#cloudpubsubconnector-configs
        """)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tempdir:
        download(tempdir)
        make_connect_config(tempdir, args.bootstrap_servers)
        run_connector(tempdir, args.connector_properties_file)


if __name__ == "__main__":
    main()
