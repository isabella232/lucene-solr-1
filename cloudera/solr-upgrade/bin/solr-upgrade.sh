#!/bin/sh

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

VALIDATION_PROCESSOR=${VALIDATION_PROCESSOR:-$(dirname "$0")/validators/solr_4_to_7_processors.xml}
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
Usage: $0 command [command-arg]
Options:
    --zk   zk_ensemble
    --debug Prints error output of calls
    --trace Prints executed commands
Commands:
  help
  download-metadata -d dest_dir
  validate-metadata -c metadata_dir
  bootstrap-config -c metadata_dir
  config-upgrade [--dry-run] -c conf_path -t conf_type -u upgrade_processor_conf -d result_dir [-v]
Parameters:
  -c <arg>     This parameter specifies the path of Solr configuration to be operated upon.
  -t <arg>     This parameter specifies the type of Solr configuration to be validated and
               transformed.The tool currently supports schema.xml, solrconfig.xml and solr.xml
  -d <arg>     This parameter specifies the directory path where the result of the command
               should be stored.
  -u <arg>     This parameter specifies the path of the Solr upgrade processor configuration.
  --dry-run    This command will perform compatibility checks for the specified Solr configuration.
  -v           This parameter enables printing XSLT compiler warnings on the command output.
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

run_config_parser_tool() {
  JVM="java"
  SCRIPT_DIR=$(dirname "$0")
  PATH="${JAVA_HOME}/bin:${PATH}" ${JVM} $JAVA_OPTS -cp "${SCRIPT_DIR}/lib/*" \
           org.apache.solr.config.upgrade.ConfigParserTool "$@" 2>&3
}

run_config_upgrade_tool() {
  JVM="java"
  SCRIPT_DIR=$(dirname "$0")
  PATH="${JAVA_HOME}/bin:${PATH}" ${JVM} $JAVA_OPTS -cp "${SCRIPT_DIR}/lib/*" \
           org.apache.solr.config.upgrade.ConfigUpgradeTool "$@" 2>&3
}

download_zk_metadata() {
  echo "Cleaning up $1"
  rm -rf "$1/*"

  mkdir -p "$1/collections"
  mkdir -p "$1/configs"

  echo "Copying clusterstate.json"
  run_zk_cli -cmd get /clusterstate.json > "$1"/clusterstate.json

  echo "Copying clusterprops.json"
  run_zk_cli -cmd get /clusterprops.json > "$1"/clusterprops.json

  echo "Copying solr.xml"
  run_zk_cli -cmd get /solr.xml > "$1"/solr.xml

  echo "Copying aliases.json"
  if ! run_zk_cli -cmd get /aliases.json > "$1"/aliases.json ; then
    echo "Unable to copy aliases.json. Please check if it contains any data ?"
    echo "Continuing with the download..."
  fi

  for c in $(run_config_parser_tool --list-collections -i "$1"/clusterstate.json); do
    echo "Downloading configuration for collection ${c}"
    run_zk_cli -cmd get /collections/"$c" > "$1/collections/${c}_config.json"
    coll_conf=$(run_config_parser_tool --get-config-name -i "$1/collections/${c}_config.json")
    echo "Downloading config named ${coll_conf} for collection ${c}"
    run_zk_cli -cmd downconfig -confdir "$1/configs/${coll_conf}/conf" -confname "${coll_conf}"
  done

  echo "Successfully downloaded SOLR metadata in Zookeeper"
}

check_dir_present() {
  if [ ! -d "$1" ]; then
    echo "$2"
    exit 1
  fi
}

check_file_present() {
  if [ ! -f "$1" ]; then
    echo "$2"
    exit 1
  fi
}

validate_metadata() {
  echo "Validating metadata in $1"

  check_file_present "$1/solr.xml" "solr.xml is missing in $1"
  check_file_present "$1/clusterstate.json" "clusterstate.json is missing in $1"
  check_file_present "$1/clusterprops.json" "clusterprops.json is missing in $1"
  check_dir_present "$1/configs" "A directory containing solr configsets is missing in $1"
  check_dir_present "$1/collections" "A directory containing solr collection configurations is missing in $1"

  configs=()
  for c in $(run_config_parser_tool --list-collections -i "$1"/clusterstate.json); do
    check_file_present "$1/collections/"$c"_config.json" "missing configuration for collection $c in $1/collections"
    coll_conf=$(run_config_parser_tool --get-config-name -i "$1/collections/${c}_config.json")
    check_dir_present "$1/configs/$coll_conf" "solr configset $coll_conf for collection $c is missing in $1/configs"

    check_file_present "$1/configs/$coll_conf/conf/solrconfig.xml" "solrconfig.xml is missing in $1/configs/$coll_conf"
    schemaFile="$1/configs/$coll_conf/conf/schema.xml"
    if [ ! -f "${schemaFile}" ]; then
      schemaFile="$1/configs/$coll_conf/conf/managed-schema"
    fi
    check_file_present "${schemaFile}" "schema file (managed-schema or schema.xml) is missing in $1/configs/$coll_conf"

    configs+=("$coll_conf")
  done

  echo "validating solr configuration using config upgrade processor @ ${VALIDATION_PROCESSOR}"

  echo "---------- validating solr.xml ----------"
  run_config_upgrade_tool --dry-run -t solrxml -c "$1/solr.xml" \
                           -u "${VALIDATION_PROCESSOR}"
  echo "---------- validation successful for solr.xml ----------"

  for c in "${configs[@]}"; do
    schemaFile="$1/configs/$c/conf/schema.xml"
    if [ ! -f "${schemaFile}" ]; then
      schemaFile="$1/configs/$c/conf/managed-schema"
    fi

    echo "---------- validating configset ${c} ----------"
    run_config_upgrade_tool --dry-run -t solrconfig -c "$1/configs/$c/conf/solrconfig.xml" \
                           -u "${VALIDATION_PROCESSOR}"
    run_config_upgrade_tool --dry-run -t schema -c "${schemaFile}" \
                           -u "${VALIDATION_PROCESSOR}"
    echo "---------- validation successful for configset ${c} ----------"
  done

  echo "Validation successful for metadata in $1"
  exit 0
}

bootstrap_config() {
  echo "Re-initializing the Solr metadata in Zookeeper"
  solrctl init --force

  echo "Copying solr.xml"
  run_zk_cli -cmd clear /solr.xml
  run_zk_cli -cmd putfile /solr.xml "$1/solr.xml"

  echo "Copying clusterprops.json"
  run_zk_cli -cmd putfile /clusterprops.json "$1/clusterprops.json"

  for config in "$1"/configs/*; do
    c="${config##*/}"
    echo "Uploading config $c"
    run_zk_cli -cmd upconfig --confdir "$1/configs/$c/conf" --confname "$c"
  done

  echo "Re-initialized Solr metadata using the configuration available at $1"
}

# First eat up all the global options
while test $# != 0 ; do
  case "$1" in
    --debug)
      exec 3>&1
      shift 1
      ;;
    --trace)
      set -x
      shift 1
      ;;
    --zk)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      SOLR_ZK_ENSEMBLE="$2"
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
    download-metadata)
      destDir=
      shift 1
      while test $# -gt 0 ; do
        case "$1" in
          -d)
            [ $# -gt 1 ] || usage "Error:  download-metadata command parameter $1 requires an argument"
            destDir="$2"
            shift 2
            ;;
          *)
            break
            ;;
        esac
      done

      if [ -z "${destDir}" ]; then
        echo "Please specify destination directory using -d option"
        usage
        exit 1
      fi

      download_zk_metadata "${destDir}"
      ;;
    validate-metadata)
      metadataDir=
      shift 1
      while test $# -gt 0 ; do
        case "$1" in
          -c)
            [ $# -gt 1 ] || usage "Error:  validate-metadata command parameter $1 requires an argument"
            metadataDir="$2"
            shift 2
            ;;
          *)
            break
            ;;
        esac
      done

      if [ -z "${metadataDir}" ]; then
        echo "Please specify metadata directory to validate using -c option"
        usage
        exit 1
      fi

      validate_metadata "${metadataDir}"
      ;;
    bootstrap-config)
      metadataDir=
      shift 1
      while test $# -gt 0 ; do
        case "$1" in
          -c)
            [ $# -gt 1 ] || usage "Error:  bootstrap-config command parameter $1 requires an argument"
            metadataDir="$2"
            shift 2
            ;;
          *)
            break
            ;;
        esac
      done

      if [ -z "${metadataDir}" ]; then
        echo "Please specify metadata directory to validate using -c option"
        usage
        exit 1
      fi

      bootstrap_config "${metadataDir}"
      ;;
    config-upgrade)
      shift 1
      dryRun=
      confPath=
      confType=
      upgradeProcessorConf=
      resultDir=
      verbose=
      while test $# -gt 0 ; do
        case "$1" in
          --dry-run|-v)
            dryRun="$1"
            shift 1
            ;;
          -c)
            [ $# -gt 1 ] || usage "Error:  config-upgrade command parameter $1 requires an argument"
            confPath="$2"
            shift 2
            ;;
          -t)
            [ $# -gt 1 ] || usage "Error:  config-upgrade command parameter $1 requires an argument"
            confType="$2"
            shift 2
            ;;
          -u)
            [ $# -gt 1 ] || usage "Error:  config-upgrade command parameter $1 requires an argument"
            upgradeProcessorConf="$2"
            shift 2
            ;;
          -d)
            [ $# -gt 1 ] || usage "Error:  config-upgrade command parameter $1 requires an argument"
            resultDir="$2"
            shift 2
            ;;
          -v)
            verbose="$2"
            shift 1
            ;;
          *)
            break
            ;;
        esac
      done

      if [ -z "${confType}" ]; then
        echo "Please specify Solr configuration type using -t option"
        usage
        exit 1
      fi
      if [ -z "${confPath}" ]; then
        echo "Please specify Solr configuration path using -c option"
        usage
        exit 1
      fi
      if [ -z "${upgradeProcessorConf}" ]; then
        echo "Please specify Solr upgrade processor config file using -u option"
        usage
        exit 1
      fi
      if [ -z "${resultDir}" ]; then
        echo "Please specify result directory path using -d option"
        usage
        exit 1
      fi

      run_config_upgrade_tool "${dryRun}" -t "${confType}" -c "${confPath}" -u "${upgradeProcessorConf}" -d "${resultDir}" "${verbose}"
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
