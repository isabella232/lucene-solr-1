#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -x

echo "Running this command as user $(whoami)"

if [ -z "${CDH_SOLR_HOME}" ]; then
  if [ -f "$(dirname "$0")/../bin/zkcli.sh" ]; then
    CDH_SOLR_HOME="$(dirname "$0")/.."
  else
    echo "Please specify location of SOLR in CDH using CDH_SOLR_HOME environment variable"
    echo "e.g. in a parcel based deployment the value would be /opt/cloudera/parcels/CDH/lib/solr"
    exit 1
  fi
fi

# Autodetect JAVA_HOME if not defined
if [ -e ${CDH_SOLR_HOME}/../../libexec/bigtop-detect-javahome ]; then
  . ${CDH_SOLR_HOME}/../../libexec/bigtop-detect-javahome
elif [ -e ${CDH_SOLR_HOME}/../bigtop-utils/bigtop-detect-javahome ]; then
  . ${CDH_SOLR_HOME}/../bigtop-utils/bigtop-detect-javahome
fi

SOLR_SVC_CMD="${CDH_SOLR_HOME}/bin/solr"
if [ ! -f "${SOLR_SVC_CMD}" ]; then
  echo "Unable to find SOLR server startup script."
  exit 1
fi

if [ -z "${SOLR_METADATA_DIR}" ]; then
  echo "Please specify a directory path providing SOLR configuration using SOLR_METADATA_DIR environment variable "
  exit 1
fi

if [ -z "${CMD_OP_DIR}" ]; then
  echo "Please specify a directory path to store the results of this command using CMD_OP_DIR environment variable "
  exit 1
fi

# Update the specified Solr configs to use local file-system (instead of HDFS).
mkdir -p  "${CMD_OP_DIR}"/config_metadata
cp -r "${SOLR_METADATA_DIR}"/* "${CMD_OP_DIR}"/config_metadata
SOLR_CONFIG_METADATA="${CMD_OP_DIR}"/config_metadata

for config in $(ls "${SOLR_CONFIG_METADATA}"/configs)
  do
    echo "Updating $config configset to use local file-system (instead of HDFS)"
    "$(dirname "$0")"/solr-upgrade.sh --debug --trace config-upgrade \
                                                     -c "${SOLR_METADATA_DIR}"/configs/"${config}"/conf/solrconfig.xml \
                                                     -t solrconfig \
                                                     -u "$(dirname "$0")"/validators/customplugins/processor.xml \
                                                     -d "${SOLR_CONFIG_METADATA}"/configs/"${config}"/conf
  done

# Start a Solr server and test these configs
SOLR_PORT=${SOLR_PORT:-8983}
SOLR_HOME_DIR="${CMD_OP_DIR}/home"
# Use Solr server log directory so as to facilitate log inspection via CM UI.
export SOLR_LOGS_DIR=${SOLR_LOG:-/var/log/solr}
export SOLR_LOG4J_CONFIG=${SOLR_LOG4J_CONFIG:-/etc/solr/conf/log4j.properties}
export SOLR_PID_DIR="${CMD_OP_DIR}"
export SOLR_SERVER_DIR="${CDH_SOLR_HOME}/server" # by default CM generates incorrect server directory path (Ref: OPSAPS-24444)
export JAVA_OPTS="-Dlog4j.configuration=file://$SOLR_LOG4J_CONFIG"
export SOLR_SSL_ENABLED="false"

# Unset the SOLR_STOP_WAIT variable configured as part of OPSAPS-37922
# This is required since the Solr server started in this script is not managed by CM and we want
# to ensure Solr process is terminated in a timely basis.
unset SOLR_STOP_WAIT

if [ -n "${SOLR_PLUGINS_DIR}" ]; then
  if [ ! -d "${SOLR_PLUGINS_DIR}" ]; then
    echo "The solr plugins directory ${SOLR_PLUGINS_DIR} does not exist!"
    exit 1
  else
    export JAVA_OPTS="${JAVA_OPTS} -Dsolr.plugins.dir=${SOLR_PLUGINS_DIR}"
  fi
fi

mkdir -p "${SOLR_HOME_DIR}"

# Make sure to stop the test instance at the end
function stop_svc {
  "${SOLR_SVC_CMD}" stop -p "${SOLR_PORT}"
}
trap stop_svc EXIT

# Copy solr.xml
echo "Copying solr.xml to ${SOLR_HOME_DIR}"
cp "${SOLR_METADATA_DIR}"/solr.xml ${SOLR_HOME_DIR}

# Start SOLR server
echo "Starting SOLR instance"
"${SOLR_SVC_CMD}" start -p "${SOLR_PORT}" -s "${SOLR_HOME_DIR}" -a "$JAVA_OPTS"

# Test configsets by creating cores on the test instance
for config in $(ls "${SOLR_CONFIG_METADATA}"/configs)
  do
    echo "Testing configset $config"
    "${SOLR_SVC_CMD}" create_core -c "$config" -d "${SOLR_CONFIG_METADATA}/configs/$config" -p "${SOLR_PORT}"
  done

echo "Verification successful. GoodBye!"
