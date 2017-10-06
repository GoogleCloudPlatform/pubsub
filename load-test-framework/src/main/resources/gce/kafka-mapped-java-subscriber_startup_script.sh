#/bin/bash

#######################################
# Query GCE for a provided metadata field.
# See https://developers.google.com/compute/docs/metadata
# Globals:
#   None
# Arguments:
#   $1: The path to the metadata field to retrieve
# Returns:
#   The value stored at the metadata field
#######################################
function metadata() {
  curl --silent --show-error --header 'Metadata-Flavor: Google' \
    "http://metadata/computeMetadata/v1/${1}";
}

readonly TMP="$(mktemp -d)"
readonly BUCKET=$(metadata instance/attributes/bucket)

[[ "${TMP}" != "" ]] || error mktemp failed

# Download the loadtest binary to this machine and install Java 8.
/usr/bin/apt-get update
/usr/bin/apt-get install -y openjdk-8-jre-headless & PIDAPT=$!
/usr/bin/gsutil cp "gs://${BUCKET}/driver.jar" "${TMP}"
export GOOGLE_CLOUD_PROJECT=dataproc-kafka-test

wait $PIDAPT

# Run the loadtest binary
java -Xmx5000M -cp ${TMP}/driver.jar com.google.pubsub.clients.mapped.CPSSubscriberTask
