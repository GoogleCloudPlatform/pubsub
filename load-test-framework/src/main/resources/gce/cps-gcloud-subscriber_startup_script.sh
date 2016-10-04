#/bin/bash

readonly TMP="$(mktemp -d)"
[[ "${TMP}" != "" ]] || error mktemp failed

# Download the loadtest binary to this machine.
/usr/bin/gsutil cp "gs://cloud-pubsub-loadtest/gcloud_subscriber.jar" "${TMP}"

# Run the loadtest binary
java -Xmx5000M -jar ${TMP}/gcloud_subscriber.jar
