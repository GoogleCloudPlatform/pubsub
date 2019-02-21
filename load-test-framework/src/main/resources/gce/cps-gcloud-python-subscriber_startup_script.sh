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

# Download the loadtest binary to this machine and install python.
apt-get update
apt-get install -y unzip
gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}"

# set up miniconda environment
cd ${TMP}
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh
chmod 755 miniconda.sh
./miniconda.sh -b -p /miniconda
source /miniconda/bin/activate
conda install -y python=3.7 pip

# unpack loadtest and install requirements
unzip cps.zip
cd python_src
python3.7 -m pip install -r requirements.txt

# run loadtest server
echo "running server..."
python3.7 -m clients.cps_subscriber_task &
