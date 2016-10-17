#/bin/bash

readonly TMP="$(mktemp -d)"
[[ "${TMP}" != "" ]] || error mktemp failed

# Download the loadtest binary to this machine and install Java 8.
/usr/bin/apt-get install -y openjdk-8-jre-headless & PIDAPT=$!
/usr/bin/gsutil cp "gs://cloud-pubsub-loadtest/driver.jar" "${TMP}" & PIDDRIV=$1
/usr/bin/gsutil cp "gs://cloud-pubsub-loadtest/cps.zip" "${TMP}" & PIDPY=$1
cd ${TMP}
unzip cps.zip
pip install -r requirements.txt
python cps_publisher_task.py

wait $PIDAPT
wait $PIDDRIV

# Run the loadtest binary
java -Xmx5000M -cp ${TMP}/driver.jar com.google.pubsub.clients.gcloud.CPSPublisherTask
