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

SOLRCTL_OPTS=""

JAAS_PARAM=""

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

# Ensure that LOG4J_PROPS env variable is configured automatically by the
# Solr upgrade tool.
if [ -z "${LOG4J_PROPS}" ]; then
  if [ -f "${SOLR_CONF_DIR}/log4j.properties" ]; then
    export LOG4J_PROPS="${SOLR_CONF_DIR}/log4j.properties"
  else
    export LOG4J_PROPS="$(dirname "$0")/lib/log4j.properties"
  fi
else
  export LOG4J_PROPS="${LOG4J_PROPS}"
fi

usage() {
  echo "
Usage: $0 command [command-arg]
Options:
    --zk   zk_ensemble
    --jaas jaas.conf
    --debug Prints error output of calls
    --trace Prints executed commands
Commands:
  help
  download-metadata -d dest_dir
  validate-metadata -c metadata_dir
  bootstrap-config -c metadata_dir
  config-upgrade [--dry-run] -c conf_path -t conf_type -u upgrade_processor_conf -d result_dir [-v]
  bootstrap-collections -c metadata_folder_path -d local_work_dir -h hdfs_work_dir
Parameters:
  -c <arg>     This parameter specifies the path of Solr configuration to be operated upon.
  -t <arg>     This parameter specifies the type of Solr configuration to be validated and
               transformed.The tool currently supports schema.xml, solrconfig.xml and solr.xml.
               The valid values for this parameter are schema, solrconfig and solrxml.
  -d <arg>     This parameter specifies the directory path where the result of the command
               should be stored.
  -h <arg>     This parameter specifies the HDFS directory path where the result of the command
               should be stored on HDFS. Eg. /solr-backup
  -u <arg>     This parameter specifies the path of the Solr upgrade processor configuration.
  --dry-run    This command will perform compatibility checks for the specified Solr configuration.
  -v           This parameter enables printing XSLT compiler warnings on the command output.
"
}

run_solrctl() {
  solrctl ${SOLRCTL_OPTS} "$@"
}

run_zk_cli() {
  ZKCLI_JVM_FLAGS_CDH="$(jaas_zkcli_flags ${1})"
  shift;

  if [ -n "$solrZkEnsemble" ]; then
    export SOLR_ZK_ENSEMBLE="$solrZkEnsemble"
  fi

  : ${CDH_SOLR_HOME:?"Please configure CDH_SOLR_HOME environment variable"}
  : ${SOLR_ZK_ENSEMBLE:?"Please configure Solr Zookeeper ensemble via -z parameter"}
  ZKCLI_JVM_FLAGS=${ZKCLI_JVM_FLAGS_CDH} "${CDH_SOLR_HOME}"/bin/zkcli.sh -zkhost $SOLR_ZK_ENSEMBLE "$@" 2>&3
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
  run_zk_cli C5 -cmd get /clusterstate.json > "$1"/clusterstate.json

  echo "Copying clusterprops.json"
  run_zk_cli C5 -cmd get /clusterprops.json > "$1"/clusterprops.json

  echo "Copying solr.xml"
  run_zk_cli C5 -cmd get /solr.xml > "$1"/solr.xml

  echo "Copying aliases.json"
  if ! run_zk_cli C5 -cmd get /aliases.json > "$1"/aliases.json ; then
    echo "Unable to copy aliases.json. Please check if it contains any data ?"
    echo "Continuing with the download..."
  fi

  for c in $(run_config_parser_tool --list-collections -i "$1"/clusterstate.json); do
    echo "Downloading configuration for collection ${c}"
    run_zk_cli C5 -cmd get /collections/"$c" > "$1/collections/${c}_config.json"
    coll_conf=$(run_config_parser_tool --get-config-name -i "$1/collections/${c}_config.json")
    echo "Downloading config named ${coll_conf} for collection ${c}"
    run_zk_cli C5 -cmd downconfig -confdir "$1/configs/${coll_conf}/conf" -confname "${coll_conf}"
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
  run_solrctl init --force

  echo "Copying solr.xml"
  run_zk_cli C6 -cmd clear /solr.xml
  run_zk_cli C6 -cmd putfile /solr.xml "$1/solr.xml"

  echo "Copying clusterprops.json"
  run_zk_cli C6 -cmd putfile /clusterprops.json "$1/clusterprops.json"

  for config in "$1"/configs/*; do
    if [ -d "${config}" ]; then
      c="${config##*/}"
      echo "Uploading config $c"
      run_zk_cli C6 -cmd upconfig --confdir "$1/configs/$c/conf" --confname "$c"
    fi
  done

  echo "Re-initialized Solr metadata using the configuration available at $1"
}

bootstrap_collections() {
  SOURCEDIR=$1
  WORKDIR=$2
  HDFS_WORKDIR=$3
  DT=`date`
  echo "Cleaning up ${WORKDIR}"
  rm -rf "${WORKDIR}/*"

  echo "---------- Generating backup-formatted directories... ----------"
  for c in $(run_config_parser_tool --list-collections -i "${SOURCEDIR}"/clusterstate.json); do
    echo "Generating backup-format for collection ${c}"
    coll_conf=$(run_config_parser_tool --get-config-name -i "${SOURCEDIR}/collections/${c}_config.json")
    mkdir -p "${WORKDIR}/${c}/zk_backup/configs"
    echo "Copy config named ${coll_conf} for collection ${c}"
    cp -R "${SOURCEDIR}/configs/${coll_conf}/conf" "${WORKDIR}/${c}/zk_backup/configs/${coll_conf}"
    echo "Generating backup.properties for collection ${c}"
    cat > "${WORKDIR}/${c}/backup.properties" <<__EOT__
backupName=${c}
index.version=4.10.3
collection.configName=${coll_conf}
startTime=${DT}
collection=${c}
__EOT__
    echo "Generating collection_state.json for collection ${c}"
    run_config_parser_tool --get-collection-state -i "${SOURCEDIR}"/clusterstate.json -c "${c}" \
      > "${WORKDIR}/${c}/zk_backup/collection_state.json"
  done
  echo "---------- Successfully built backup-formatted directories ----------"

  echo "---------- Uploading backup directories to ${HDFS_WORKDIR} ----------"

  hdfs_cmd -mkdir -p "${HDFS_WORKDIR}/"
  hdfs_cmd -put -f $WORKDIR/* "${HDFS_WORKDIR}/"

  SUFFIX=$(random_string)

  allSuccess=true
  set +e
  for c in $(run_config_parser_tool --list-collections -i "${SOURCEDIR}"/clusterstate.json); do
    echo "---------- Re-initializing ${c} ----------"
    # Starting restore command
    run_solrctl collection --restore "${c}" -b "${c}" -l "${HDFS_WORKDIR}/" -i "restore-${c}-${SUFFIX}"
    if [ $? != 0 ]; then
       allSuccess=false
       echo "Re-initialization of ${c} FAILED."
       continue
    fi
    # Waiting for async restore to finish
    while true;
    do
        run_solrctl collection --request-status "restore-${c}-${SUFFIX}" | egrep -q '"state":"running"' || break
        sleep 1
        echo "initializing..."
    done
    # Checking final status
    $(run_solrctl collection --request-status "restore-${c}-${SUFFIX}"| egrep -q '"state":"completed"')
    if [ $? != 0 ]; then
      allSuccess=false
      echo "Re-initialization of ${c} FAILED. Run the following for details: solrctl collection --request-status restore-${c}-${SUFFIX}"
      continue
    fi
    # If all went well
    echo "---------- Re-initialization of ${c} completed successfully. ----------"
  done
  set -e

  if [ $allSuccess != true ]; then
    echo "FAILURE: Not all of the collections were successfully restored."
    exit -1
  fi

  #TODO Should we clean up?
}

hdfs_cmd(){
  HDFS_CMD="${HDFS_BIN:-hdfs}"
  HDFS_CONFIG_PARAM=""
  if [ -n "${SOLR_HDFS_CONFIG}" ]; then
    HDFS_CONFIG_PARAM="--config ${SOLR_HDFS_CONFIG}"
  fi
  ${HDFS_CMD} ${HDFS_CONFIG_PARAM} dfs "$@"
}

random_string(){
  head -c 64 /dev/urandom|shasum|head -c 8
}

jaas_zkcli_flags(){
  if [ -z $JAAS_PARAM ]; then
    echo "${ZKCLI_JVM_FLAGS}"
  else
    case "$1" in
      C5)
        ZK_ACL_PROVIDER=${ZK_ACL_PROVIDER:-"org.apache.solr.common.cloud.ConfigAwareSaslZkACLProvider"}
        ;;
      C6)
        ZK_ACL_PROVIDER=${ZK_ACL_PROVIDER:-"org.apache.solr.common.cloud.SaslZkACLProvider"}
        ;;
      *)
        echo "Invalid version ${1}"
        exit 1;
        ;;
    esac
    echo "-Djava.security.auth.login.config=${JAAS_PARAM} -DzkACLProvider=${ZK_ACL_PROVIDER} ${ZKCLI_JVM_FLAGS}"
  fi
}

# First eat up all the global options
while test $# != 0 ; do
  case "$1" in
    --debug)
      exec 3>&1
      shift 1
      SOLRCTL_OPTS="${SOLRCTL_OPTS} --debug"
      ;;
    --trace)
      set -x
      shift 1
      SOLRCTL_OPTS="${SOLRCTL_OPTS} --trace"
      ;;
    --zk)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      SOLR_ZK_ENSEMBLE="$2"
      SOLRCTL_OPTS="${SOLRCTL_OPTS} --zk $SOLR_ZK_ENSEMBLE"
      shift 2
      ;;
    --jaas)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      [ -e "$2" ] || usage "Error: $2 must be a file"
      JAAS_PARAM="${2}"
      SOLRCTL_OPTS="${SOLRCTL_OPTS} --jaas ${2}"
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

      check_dir_present "${destDir}" "Specified output directory path ${destDir} does not exist!"
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

      check_dir_present "${metadataDir}" "Specified metadata directory path ${metadataDir} does not exist!"
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

      check_dir_present "${metadataDir}" "Specified metadata directory path ${metadataDir} does not exist!"

      bootstrap_config "${metadataDir}"
      ;;
    bootstrap-collections)
      metadataDir=
      workDir=
      hdfsDir=
      shift 1
      while test $# -gt 0 ; do
        case "$1" in
          -c)
            [ $# -gt 1 ] || usage "Error:  bootstrap-collections command parameter $1 requires an argument"
            metadataDir="$2"
            shift 2
            ;;
          -d)
            [ $# -gt 1 ] || usage "Error:  bootstrap-collections command parameter $1 requires an argument"
            workDir="$2"
            shift 2
            ;;
          -h)
            [ $# -gt 1 ] || usage "Error:  bootstrap-collections command parameter $1 requires an argument"
            hdfsDir="$2"
            shift 2
            ;;
          *)
            break
            ;;
        esac
      done

      if [ -z "${metadataDir}" ]; then
        echo "Please specify metadata directory using -c option"
        usage
        exit 1
      fi
      if [ -z "${workDir}" ]; then
        echo "Please specify a working directory using -d option"
        usage
        exit 1
      fi
      if [ -z "${hdfsDir}" ]; then
        echo "Please specify an HDFS working directory using -h option"
        usage
        exit 1
      fi

      check_dir_present "${metadataDir}" "Specified metadata directory path ${metadataDir} does not exist!"
      check_dir_present "${workDir}" "Specified work directory path ${workDir} does not exist!"

      bootstrap_collections "${metadataDir}" "${workDir}" "${hdfsDir}"
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
