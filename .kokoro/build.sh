#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

## Get the directory of the build script
scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))
## cd to the parent directory, i.e. the root of the git repo
cd ${scriptDir}/..

# include common functions
source ${scriptDir}/common.sh

# Print out Java version
java -version
echo ${JOB_TYPE}

# attempt to install 3 times with exponential backoff (starting with 10 seconds)
retry_with_backoff 3 10 \
  mvn install -B -V \
    -DskipTests=true \
    -Dclirr.skip=true \
    -Denforcer.skip=true \
    -Dmaven.javadoc.skip=true \
    -Dgcloud.download.skip=true \
    -T 1C

# if GOOGLE_APPLICATION_CREDIENTIALS is specified as a relative path prepend Kokoro root directory onto it
if [[ ! -z "${GOOGLE_APPLICATION_CREDENTIALS}" && "${GOOGLE_APPLICATION_CREDENTIALS}" != /* ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS=$(realpath ${KOKORO_GFILE_DIR}/${GOOGLE_APPLICATION_CREDENTIALS})
fi

RETURN_CODE=0
set +e

case ${JOB_TYPE} in
test)
    mvn test -B -Dclirr.skip=true -Denforcer.skip=true
    RETURN_CODE=$?
    ;;
lint)
    mvn com.coveo:fmt-maven-plugin:check
    RETURN_CODE=$?
    ;;
javadoc)
    mvn javadoc:javadoc javadoc:test-javadoc
    RETURN_CODE=$?
    ;;
integration)
    if [ -f "${KOKORO_GFILE_DIR}/secret_manager/pubsub-secrets" ]
      then
          source "${KOKORO_GFILE_DIR}/secret_manager/pubsub-secrets"
    fi
    mvn clean test -Dtest=it.StandaloneIT -DfailIfNoTests=false
    RETURN_CODE=$?
    ;;
*)
    ;;
esac

if [ "${REPORT_COVERAGE}" == "true" ]
then
  bash ${KOKORO_GFILE_DIR}/codecov.sh
fi

# fix output location of logs
bash .kokoro/coerce_logs.sh

if [[ "${ENABLE_BUILD_COP}" == "true" ]]
then
    chmod +x ${KOKORO_GFILE_DIR}/linux_amd64/flakybot
    ${KOKORO_GFILE_DIR}/linux_amd64/flakybot -repo=GoogleCloudPlatform/pubsub
fi

echo "exiting with ${RETURN_CODE}"
exit ${RETURN_CODE}