#/bin/bash

readonly TMP="$(mktemp -d)"
[[ "${TMP}" != "" ]] || error mktemp failed

# Download the loadtest binary to this machine and install Java 8.
/usr/bin/apt-get update
/usr/bin/apt-get install -y openjdk-8-jre-headless unzip python3-pip & PIDAPT=$!
/usr/bin/gsutil cp "gs://cloud-pubsub-loadtest/driver.jar" "${TMP}" & PIDDRIV=$1
/usr/bin/gsutil cp "gs://cloud-pubsub-loadtest/cps.zip" "${TMP}" & PIDPY=$1

wait $PIDAPT
wait $PIDDRIV
wait $PIDPY

cd ${TMP}
unzip cps.zip
pip3 install -r requirements.txt
python3 cps_publisher_task.py &

# Run the loadtest binary
java -Xmx5000M -cp driver.jar com.google.pubsub.clients.adapter.PublisherAdapterTask
