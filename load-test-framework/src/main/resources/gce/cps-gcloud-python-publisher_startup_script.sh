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
/usr/bin/apt-get install -y openjdk-8-jre-headless unzip python3-pip git & PIDAPT=$!
/usr/bin/gsutil cp "gs://${BUCKET}/driver.jar" "${TMP}" & PIDDRIV=$1
/usr/bin/gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}" & PIDPY=$1

wait $PIDAPT
wait $PIDDRIV
wait $PIDPY

pip3 install --upgrade pip
pip3 install --upgrade setuptools

cd ${TMP}
unzip cps.zip
cd python_src/clients/
pip3 install -r requirements.txt
git clone https://github.com/GoogleCloudPlatform/google-cloud-python.git
cd google-cloud-python/pubsub/
git checkout --track -b pubsub-subscriber origin/pubsub-subscriber
pip3 install .
cd ../../
python3 cps_publisher_task.py &
cd ../../

# Run the loadtest binary
java -Xmx5000M -cp driver.jar com.google.pubsub.clients.adapter.PublisherAdapterTask
