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

# Add the correct repo for nodejs
curl -sL https://deb.nodesource.com/setup_6.x | sudo -E bash -

# Download the loadtest binary to this machine and install Java 8.
/usr/bin/apt-get update
/usr/bin/apt-get install -y openjdk-8-jre-headless unzip nodejs & PIDAPT=$!
/usr/bin/gsutil cp "gs://${BUCKET}/driver.jar" "${TMP}" & PIDDRIV=$1
/usr/bin/gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}" & PIDCPS=$1

wait $PIDAPT
wait $PIDDRIV
wait PIDCPS

cd ${TMP}
unzip cps.zip
cd node_src
npm install
node subscriber_client.js &
cd ..

# Run the loadtest binary
java -Xmx5000M -cp driver.jar com.google.pubsub.clients.adapter.SubscriberAdapterTask