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

# Download the loadtest binary to this machine and install dotnet.
#/usr/bin/apt-get update
#/usr/bin/apt-get install -y openjdk-8-jre-headless & PIDAPT=$!
/usr/bin/gsutil cp "gs://${BUCKET}/driver.jar" "${TMP}"

#wait $PIDAPT

#ulimit -n 32768

# Limit the jvm to 2/3 available memory.
#MEM="$(cat /proc/meminfo | head -n 1 | awk '{print $2}')"
#let "MEMJAVA = (MEM * 2 / 3) / 1000"

#java -Xmx${MEMJAVA}m -cp ${TMP}/driver.jar com.google.pubsub.clients.gcloud.CPSSubscriberTask

# install dotnet
curl -o preview-sdk.tgz https://download.visualstudio.microsoft.com/download/pr/35c9c95a-535e-4f00-ace0-4e1686e33c6e/b9787e68747a7e8a2cf8cc530f4b2f88/dotnet-sdk-3.0.100-preview3-010431-linux-x64.tar.gz
mkdir -p $HOME/dotnet && tar zxf preview-sdk.tgz -C $HOME/dotnet
export DOTNET_ROOT=$HOME/dotnet 
export PATH=$HOME/dotnet:$PATH
# Make older SDKs and runtimes available
for i in /usr/share/dotnet/sdk/*; do ln -s $i $DOTNET_ROOT/sdk; done
for i in /usr/share/dotnet/shared/Microsoft.AspNetCore.All/*; do ln -s $i $DOTNET_ROOT/shared/Microsoft.AspNetCore.All; done
for i in /usr/share/dotnet/shared/Microsoft.AspNetCore.App/*; do ln -s $i $DOTNET_ROOT/shared/Microsoft.AspNetCore.App; done
for i in /usr/share/dotnet/shared/Microsoft.NETCore.App/*; do ln -s $i $DOTNET_ROOT/shared/Microsoft.NETCore.App; done

cd ${TMP}
# unpack loadtest
unzip cps.zip
cd dotnet_src/Google.PubSub.Flic

# run loadtest
dotnet run