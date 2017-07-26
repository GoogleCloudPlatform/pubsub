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

# Download the loadtest binary to this machine and install Ruby
/usr/bin/apt-get update
/usr/bin/curl -sSL https://get.rvm.io | bash -s stable
source /etc/profile.d/rvm.sh
rvm install ruby-2.4.0
rvm use ruby-2.4.0
gem install bundler & PIDRVM=$1
/usr/bin/apt-get install -y openjdk-8-jre-headless unzip & PIDAPT=$1
/usr/bin/gsutil cp "gs://${BUCKET}/driver.jar" "${TMP}" & PIDDRIV=$1
/usr/bin/gsutil cp "gs://${BUCKET}/cps.zip" "${TMP}" & PIDRB=$1

wait $PIDAPT
wait $PIDRVM
wait $PIDDRIV
wait $PIDRB

cd ${TMP}
unzip cps.zip

cd ./ruby_src
/usr/local/rvm/gems/ruby-2.4.0/bin/bundle install
/usr/local/rvm/gems/ruby-2.4.0/bin/bundle exec ruby cps_subscriber_task.rb &
cd ..

# Run the loadtest binary
java -Xmx5000M -cp driver.jar com.google.pubsub.clients.adapter.SubscriberAdapterTask
