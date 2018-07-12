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

exec 3>/dev/null

set -e

SOLR_CONF_DIR=${SOLR_CONF_DIR:-/etc/solr/conf}
SOLR_DEFAULTS=${SOLR_DEFAULTS:-/etc/default/solr}
if [ -e "$SOLR_CONF_DIR/solr-env.sh" ] ; then
  . "$SOLR_CONF_DIR/solr-env.sh"
elif [ -e ${SOLR_DEFAULTS} ] ; then
  . ${SOLR_DEFAULTS}
fi

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

usage() {
  echo "
Usage: $0 command
Options:
    --zk   zk_ensemble
    --debug Prints error output of calls
    --trace Prints executed commands
Commands:
  help
  zk-meta -c conf_dir
Parameters:
  -c <arg>     This parameter specifies the path of Solr configuration to be used
"
}

run_zk_cli() {
  if [ -n "$solrZkEnsemble" ]; then
    export SOLR_ZK_ENSEMBLE="$solrZkEnsemble"
  fi

  : ${CDH_SOLR_HOME:?"Please configure CDH_SOLR_HOME environment variable"}
  : ${SOLR_ZK_ENSEMBLE:?"Please configure Solr Zookeeper ensemble via -z parameter"}
  "${CDH_SOLR_HOME}"/bin/zkcli.sh -zkhost $SOLR_ZK_ENSEMBLE "$@" 2>&3
}

SOLRCTL_ARGS=
while test $# != 0 ; do
  case "$1" in
    --debug)
      SOLRCTL_ARGS="$SOLRCTL_ARGS --debug"
      exec 3>&1
      shift 1
      ;;
    --trace)
      SOLRCTL_ARGS="$SOLRCTL_ARGS --trace"
      set -x
      shift 1
      ;;
    --zk)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      export SOLR_ZK_ENSEMBLE="$2"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

# Now start parsing commands -- there has to be at least one!
[ $# -gt 0 ] || usage
while test $# != 0 ; do
  case "$1" in
    zk-meta)
      shift 1

      backupDir=
      while test $# -gt 0 ; do
        case "$1" in
          -c)
            [ $# -gt 1 ] || usage "Error:  zk command parameter $1 requires an argument"
            backupDir="$2"
            shift 2
            ;;
          *)
            break
            ;;
        esac
      done

      if [ -z "${backupDir}" ]; then
        echo "Please specify directory containing Solr metadata using -c parameter"
        usage
        exit 1
      fi

      echo "Re-initializing the Solr metadata in Zookeeper"
      solrctl $SOLRCTL_ARGS init --force

      echo "Copying solr.xml"
      run_zk_cli -cmd clear /solr.xml
      run_zk_cli -cmd putfile /solr.xml "${backupDir}/solr.xml"
      echo "Copying clusterstate.json"
      run_zk_cli -cmd putfile /clusterstate.json "${backupDir}/clusterstate.json"
      echo "Copying clusterprops.json"
      run_zk_cli -cmd putfile /clusterprops.json "${backupDir}/clusterprops.json"

      if [ -f "${backupDir}/aliases.json" ]; then
        echo "Copying aliases.json"
        run_zk_cli -cmd putfile /aliases.json "${backupDir}"/aliases.json
      fi

      for config in "${backupDir}"/configs/*; do
        c="${config##*/}"
        echo "Uploading config $c"
        run_zk_cli -cmd upconfig --confdir "${backupDir}/configs/$c/conf" --confname "$c"
      done

      run_zk_cli -cmd makepath /collections

      for file in "${backupDir}"/collections/*; do
        c=$(echo "${file##*/}" | cut -d'_' -f 1)
        echo "Copying configuration for collection $c"
        run_zk_cli -cmd putfile "/collections/$c" "${file}"
      done

      echo "Re-initialized Solr metadata using the configuration available at ${backupDir}"
      echo "Please restart Solr for this change to take effect!"
      ;;
    help)
      usage
      shift 1
      ;;
    *)
      echo "Unknown command $1"
      usage
      exit 1
  esac
done

exit 0
