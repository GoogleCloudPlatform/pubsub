#!/bin/bash

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
curl -sL https://deb.nodesource.com/setup_11.x | sudo -E bash -

# Download the loadtest binary to this machine and install Java 8.
/usr/bin/apt-get update
/usr/bin/apt-get install -y unzip nodejs
/snap/bin/gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}"

cd ${TMP}
unzip cps.zip
cd node_src
npm install
# increase heap max to 16GB to prevent oom on large tests.
# subscriber already limits messages outstanding to 500MB per core.
node --max-old-space-size=16000 src/main.js --publisher=false
