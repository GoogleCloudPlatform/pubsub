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
export DOTNET_CLI_HOME=${TMP}

# Download the loadtest binary to this machine and install dotnet.
/usr/bin/apt-get update
/usr/bin/apt-get install -y unzip
/usr/bin/gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}"

cd ${TMP}

# install dotnet
curl -o sdk.tgz https://download.visualstudio.microsoft.com/download/pr/228832ea-805f-45ab-8c88-fa36165701b9/16ce29a06031eeb09058dee94d6f5330/dotnet-sdk-2.2.401-linux-x64.tar.gz
mkdir -p ${TMP}/dotnet
tar zxf sdk.tgz -C ${TMP}/dotnet
export PATH=${TMP}/dotnet:${PATH}

cd ${TMP}

# unpack loadtest
unzip cps.zip
cd dotnet_src/Google.PubSub.Flic

# run loadtest
dotnet run -c Release
