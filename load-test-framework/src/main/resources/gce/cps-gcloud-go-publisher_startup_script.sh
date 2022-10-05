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

# Download the loadtest binary to this machine and install Java 8.
/usr/bin/apt-get update
/usr/bin/apt-get install -y unzip gcc
/snap/bin/gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}"

cd ${TMP}

# install go
curl https://dl.google.com/go/go1.11.5.linux-amd64.tar.gz -o go.tar.gz
tar -C /usr/local -xzf go.tar.gz
export PATH=$PATH:/usr/local/go/bin
mkdir gopath
export GOPATH="${TMP}/gopath"
mkdir gocache
export GOCACHE="${TMP}/gocache"

# unpack loadtest
unzip cps.zip
cd go_src/cmd

go run main.go --publisher=true
