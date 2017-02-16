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

usage() {
  [ $# -eq 0 ] || echo "$@"
  echo "
usage: $0 [options] command [command-arg] [command [command-arg]] ...

Options:
    --solr solr_uri
    --zk   zk_ensemble
    --jaas jaas.conf
    --help
    --quiet
    --debug Prints error output of calls
    --trace Prints executed commands

Commands:
    init        [--force]

    instancedir [--generate path [-schemaless]]
                [--create name path]
                [--update name path]
                [--get name path]
                [--delete name]
                [--list]

    config      [--create name baseConfig [-p name=value]...]
                [--delete name]

    collection  [--create name -s <numShards>
                              [-a Create collection with autoAddReplicas=true]
                              [-c <collection.configName>]
                              [-r <replicationFactor>]
                              [-m <maxShardsPerNode>]
                              [-n <createNodeSet>]]
                [--delete name]
                [--reload name]
                [--stat name]
                [--deletedocs name]
                [--list]
                [--create-snapshot snapshotName -c <collectionName>]
                [--delete-snapshot snapshotName -c <collectionName>]
                [--list-snapshots collectionName]
                [--describe-snapshot snapshotName -c <collectionName>]
                [--prepare-snapshot-export snapshotName -c <collectionName> -d <destDir> [-p <fsPathPrefix>]]
                [--export-snapshot snapshotName [-s <sourceDir>] [-c <collectionName>] -d <destDir>]
                [--restore name  -b <backupName> -l <backupLocation> -i <requestId>
                                 [-a Restore collection with autoAddReplicas=true]
                                 [-c <collection.configName>]
                                 [-r <replicationFactor>]
                                 [-m <maxShardsPerNode>]]
                [--request-status requestId]

    core        [--create name [-p name=value]...]
                [--reload name]
                [--unload name]
                [--status name]

    cluster     [--get-solrxml file]
                [--put-solrxml file]
                [--set-property name value]
                [--remove-property name]
                [--get-clusterstate file]

    sentry      [--create-role role]
                [--drop-role role]
                [--add-role-group role group]
                [--delete-role-group role group]
                [--list-roles [-g group]]
                [--grant-privilege role privilege]
                [--revoke-privilege role privilege]
                [--list-privileges role]
                [--convert-policy-file file [-dry-run]]
  "
  exit 1
}

# Error codes
ERROR_GENERIC=1
ERROR_INIT_ALREADY_INITIALIZED=101
ERROR_INIT_LIVE_NODES=102

die() {
  if [ ! -z "$1" ] ; then
    echo $1
  fi
  exit ${2:-$ERROR_GENERIC}
}

# FIXME: this is here only for testing purposes
local_coreconfig() {
  case "$2" in
    put)
      echo "$4" > "/var/lib/solr/$3"
      ;;
    list)
      ls -d /var/lib/solr/*/conf 2>&3 | sed -e 's#var/lib/solr#configs#'
      ;;
    clear)
      if [ "$3" != "/" ] ; then
        sudo -u solr rm -rf /var/lib/solr/`basename $3`/* 2>&3
      fi
      ;;
    upconfig)
      # the following trick gets us around permission issues
      rm -rf /tmp/solr_conf.$$
      cp -r $4 /tmp/solr_conf.$$
      chmod o+r -R /tmp/solr_conf.$$
      sudo -u solr bash -c "mkdir /var/lib/solr/$6 2>&3 ; cp -r /tmp/solr_conf.$$ /var/lib/solr/$6/conf"
      RES=$?
      rm -rf /tmp/solr_conf.$$
      return $RES
      ;;
    downconfig)
      mkdir -p $4
      cp -r /var/lib/solr/$6/conf/* $4
      ;;
  esac 
}

solr_webapi() {
  # If SOLR_ADMIN_URI wasn't given explicitly via --solr we need to guess it
  if [ -z "$SOLR_ADMIN_URI" ] ; then
    local SOLR_PROTOCOL=`get_solr_protocol`
    for node in `get_solr_state | sed -ne 's#/live_nodes/\(.*:[0-9][0-9]*\).*$#\1#p'` localhost:$SOLR_PORT ; do
      if $SOLR_ADMIN_CURL "$SOLR_PROTOCOL://$node/solr" >&3 2>&1 ; then
        SOLR_ADMIN_URI="$SOLR_PROTOCOL://$node/solr"
        break
      fi
    done
    [ -n "$SOLR_ADMIN_URI" ] || die "Error: can't discover Solr URI. Please specify it explicitly via --solr." 
  fi

  URI="$SOLR_ADMIN_URI$1"
  shift
  local WEB_OUT=`$SOLR_ADMIN_CURL $URI "$@" | sed -e 's#>#>\n#g'`

  if [ $? -eq 0 ] && (echo "$WEB_OUT" | grep -q 'HTTP/.*200.*OK') ; then
    echo "$WEB_OUT" | egrep -q '<lst name="(failure|exception|error|status)">' || return 0
  fi

  die "Error: A call to SolrCloud WEB APIs failed: $WEB_OUT"
}

get_solr_protocol() {
  if [ -z "$SOLR_STATE" ] ; then
    SOLR_STATE=`eval $SOLR_ADMIN_ZK_CMD -cmd list`
  fi

  if echo "$SOLR_STATE" | grep -i 'urlScheme' | grep -q -i 'https' ; then
    echo "https"
  else
    echo "http"
  fi
}

get_solr_state() {
  if [ -z "$SOLR_STATE" ] ; then
    SOLR_STATE=`eval $SOLR_ADMIN_ZK_CMD -cmd list`
  fi

  echo "$SOLR_STATE" | grep -v '^/ '
}

run_solr_snapshot_tool() {
  ZKCLI_JVM_FLAGS=${ZKCLI_JVM_FLAGS} LOG4J_PROPS=${SOLR_CONF_DIR}/log4j.properties ${SOLR_HOME}/bin/snapshotscli.sh "$@"
}

SOLR_CONF_DIR=${SOLR_CONF_DIR:-/etc/solr/conf}
SOLR_DEFAULTS=${SOLR_DEFAULTS:-/etc/default/solr}
SENTRY_CONF_DIR=${SENTRY_CONF_DIR:-/etc/sentry/conf}

if [ -e "$SOLR_CONF_DIR/solr-env.sh" ] ; then
  . "$SOLR_CONF_DIR/solr-env.sh"
elif [ -e ${SOLR_DEFAULTS} ] ; then
  . ${SOLR_DEFAULTS}
else
  SOLR_PORT=8983
fi

SOLR_ADMIN_CURL='curl -i --retry 5 -s -L -k --negotiate -u :'
SOLR_ADMIN_CHAT=echo
SOLR_ADMIN_API_CMD='solr_webapi'

HADOOP_HOME=${HADOOP_HOME:-/usr/lib/hadoop/}
SOLR_HOME=${SOLR_HOME:-/usr/lib/solr/}

# Autodetect JAVA_HOME if not defined
if [ -e ${SOLR_HOME}/../../libexec/bigtop-detect-javahome ]; then
  . ${SOLR_HOME}/../../libexec/bigtop-detect-javahome
elif [ -e ${SOLR_HOME}/../bigtop-utils/bigtop-detect-javahome ]; then
  . ${SOLR_HOME}/../bigtop-utils/bigtop-detect-javahome
fi


# First eat up all the global options

while test $# != 0 ; do
  case "$1" in 
    --help)
      usage
      ;;
    --quiet)
      SOLR_ADMIN_CHAT=/bin/true
      shift 1
      ;;
    --debug)
      export SOLR_DEBUG="true"
      exec 3>&1
      shift 1
      ;;
    --trace)
      export SOLR_TRACE="true"
      set -x
      shift 1
      ;;
    --solr)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      SOLR_ADMIN_URI="$2"
      shift 2
      ;;
    --zk)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      SOLR_ZK_ENSEMBLE="$2"
      shift 2
      ;;
    --jaas)
      [ $# -gt 1 ] || usage "Error: $1 requires an argument"
      [ -e "$2" ] || usage "Error: $2 must be a file"
      ZKCLI_JVM_FLAGS="-Djava.security.auth.login.config=$2 -DzkACLProvider=org.apache.solr.common.cloud.ConfigAwareSaslZkACLProvider ${ZKCLI_JVM_FLAGS}"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

if [ -n "$SOLR_AUTHORIZATION_SUPERUSER" ] ; then
  ZKCLI_JVM_FLAGS="-Dsolr.authorization.superuser=${SOLR_AUTHORIZATION_SUPERUSER} ${ZKCLI_JVM_FLAGS}"
fi

if [ -n "$ZK_SASL_CLIENT_USERNAME" ] ; then
  ZKCLI_JVM_FLAGS="-Dzookeeper.sasl.client.username=${ZK_SASL_CLIENT_USERNAME} ${ZKCLI_JVM_FLAGS}"
fi

if [ -n "$ZKCLI_TMPDIR" ] ; then
  ZKCLI_JVM_FLAGS="-Djava.io.tmpdir=${ZKCLI_TMPDIR} ${ZKCLI_JVM_FLAGS}"
fi

if [ -z "$SOLR_ZK_ENSEMBLE" ] ; then
  SOLR_ADMIN_ZK_CMD="local_coreconfig"
  cat >&2 <<-__EOT__
	Warning: Non-SolrCloud mode has been completely deprecated
	Please configure SolrCloud via SOLR_ZK_ENSEMBLE setting in 
	SOLR_DEFAULTS file
	If you running remotely, please use --zk zk_ensemble.
	__EOT__
else
  SOLR_ADMIN_ZK_CMD='ZKCLI_JVM_FLAGS=${ZKCLI_JVM_FLAGS} LOG4J_PROPS=${SOLR_CONF_DIR}/log4j.properties ${SOLR_HOME}/bin/zkcli.sh -zkhost $SOLR_ZK_ENSEMBLE 2>&3'
fi

# Now start parsing commands -- there has to be at least one!
[ $# -gt 0 ] || usage 
while test $# != 0 ; do
  case "$1" in 
    debug-dump)
      get_solr_state

      shift 1
      ;;

    init)
      if [ "$2" == "--force" ] ; then
        shift 1
      else
        LIVE_NODES=`get_solr_state | sed -ne 's#/live_nodes/##p'`

        if [ -n "$LIVE_NODES" ] ; then
          die "Warning: It appears you have live SolrCloud nodes running: `printf '\n%s\nPlease shut them down.' \"${LIVE_NODES}\"`" $ERROR_INIT_LIVE_NODES
        elif [ -n "`get_solr_state`" ] ; then
          die "Warning: Solr appears to be initialized (use --force to override)" $ERROR_INIT_ALREADY_INITIALIZED
        fi
      fi

      eval $SOLR_ADMIN_ZK_CMD -cmd makepath / 1>&3 2>&1 || :
      eval $SOLR_ADMIN_ZK_CMD -cmd clear /    || die "Error: failed to initialize Solr"

      eval $SOLR_ADMIN_ZK_CMD -cmd putfile /solr.xml ${SOLR_HOME}/clusterconfig/solr.xml
      eval $SOLR_ADMIN_ZK_CMD -cmd makepath /configs
      for DIRNAME in predefinedTemplate managedTemplate schemalessTemplate predefinedTemplateSecure managedTemplateSecure schemalessTemplateSecure
      do
        eval $SOLR_ADMIN_ZK_CMD -cmd upconfig -confdir ${SOLR_HOME}/$DIRNAME -confname $DIRNAME
      done

      shift 1
      ;;

    coreconfig)
      $SOLR_ADMIN_CHAT  "Warning: coreconfig is deprecated, please use instancedir instead (consult documentation on differences in behaviour)."
      shift 1
      set instancedir "$@"
      ;;
    instancedir)
      [ $# -gt 1 ] || usage "Error: incorrect specification of arguments for $1"
      case "$2" in
        --create) 
            [ $# -gt 3 ] || usage "Error: incorrect specification of arguments for $1 $2" 

            if [ -d $4/conf ] ; then
              INSTANCE_DIR="$4/conf"
            else
              INSTANCE_DIR="$4"
            fi

            [ -e ${INSTANCE_DIR}/solrconfig.xml -a -e ${INSTANCE_DIR}/schema.xml ] || die "Error: ${INSTANCE_DIR} must be a directory with at least solrconfig.xml and schema.xml"

            get_solr_state | grep -q '^ */configs/'"$3/" && die "Error: \"$3\" configuration already exists. Aborting. Try --update if you want to override"

            $SOLR_ADMIN_CHAT "Uploading configs from ${INSTANCE_DIR} to $SOLR_ZK_ENSEMBLE. This may take up to a minute."
            eval $SOLR_ADMIN_ZK_CMD -cmd upconfig -confdir ${INSTANCE_DIR} -confname $3 || die "Error: can't upload configuration"
            shift 4
            ;;
        --update)
            [ $# -gt 3 ] || usage "Error: incorrect specification of arguments for $1 $2"

            if [ -d $4/conf ] ; then
              INSTANCE_DIR="$4/conf"
            else
              INSTANCE_DIR="$4"
            fi

            [ -e ${INSTANCE_DIR}/solrconfig.xml -a -e ${INSTANCE_DIR}/schema.xml ] || die "Error: ${INSTANCE_DIR} must be a directory with at least solrconfig.xml and schema.xml"

            eval $SOLR_ADMIN_ZK_CMD -cmd clear /configs/$3 || die "Error: can't delete configuration"

            $SOLR_ADMIN_CHAT "Uploading configs from ${INSTANCE_DIR} to $SOLR_ZK_ENSEMBLE. This may take up to a minute."
            eval $SOLR_ADMIN_ZK_CMD -cmd upconfig -confdir ${INSTANCE_DIR} -confname $3 || die "Error: can't upload configuration"
            shift 4
            ;;
        --get)
            [ $# -gt 3 ] || usage "Error: incorrect specification of arguments for $1 $2"

            [ -e "$4" ] && die "Error: subdirectory $4 already exists"

            $SOLR_ADMIN_CHAT "Downloading configs from $SOLR_ZK_ENSEMBLE to $4. This may take up to a minute."
            eval $SOLR_ADMIN_ZK_CMD -cmd downconfig -confdir "$4/conf" -confname "$3" || die "Error: can't download configuration"
            shift 4
            ;;
        --delete)
            [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"
            
            eval $SOLR_ADMIN_ZK_CMD -cmd clear /configs/$3 || die "Error: can't delete configuration"
            shift 3
            ;;
        --generate)
            [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"

            [ -e "$3" ] && die "Error: subdirectory $3 already exists"
            schemaless="false"
            if [ $# -gt 3 -a "$4" = "-schemaless" ] ; then
              schemaless="true"
            fi
            mkdir -p "$3" >&3 2>&1
            [ -d "$3" ] || usage "Error: $3 has to be a directory"
            cp -r ${SOLR_HOME}/coreconfig-template "$3/conf"
            if [ "$schemaless" = "true" ] ; then
              [ -d "$3"/conf ] || die "Error: subdirectory $3/conf must exist"
              cp ${SOLR_HOME}/coreconfig-schemaless-template/schema.xml.schemaless "$3/conf/schema.xml"
              cp ${SOLR_HOME}/coreconfig-schemaless-template/solrconfig.xml.schemaless "$3/conf/solrconfig.xml"
              cp ${SOLR_HOME}/coreconfig-schemaless-template/solrconfig.xml.schemaless.secure "$3/conf/solrconfig.xml.secure"
              shift 4
            else
              shift 3
            fi
            ;;
        --list)
            get_solr_state | sed -n -e '/\/configs\//s#^.*/configs/\([^/]*\)/.*$#\1#p' | sort -u
            shift 2
            ;;
        *)  
            shift 1
            ;;
      esac
      ;;

    config)
      [ $# -gt 2 ] || usage "Error: incorrect specifications of arguments for $1"
      case "$2" in
        --create)
          [ $# -gt 3 ] || usage "Error: config --create name requires a base config name"
          CONF_CREATE_NAME="$3"
          CONF_CREATE_BASE_NAME="$4"
          shift 4
          while test $# -gt 0 ; do
            case "$1" in
              -p)
                [ $# -gt 1 ] || usage "Error: config --create name $1 requires an argument of key=value"
                CONF_KV_PAIRS="${CONF_KV_PAIRS}&configSetProp.${2}"
                shift 2
                ;;
               *)
                break
                ;;
            esac
          done

          eval $SOLR_ADMIN_API_CMD "'/admin/configs?action=CREATE&name=${CONF_CREATE_NAME}&baseConfigSet=${CONF_CREATE_BASE_NAME}${CONF_KV_PAIRS}'"
          ;;
        --delete)
            eval $SOLR_ADMIN_API_CMD "'/admin/configs?action=DELETE&name=$3'"
            shift 3
            ;;
        *)
            shift 1
            ;;
      esac
      ;;
    collection)
      [ "$2" = "--list" ] || [ "$2" = "--list-snapshots" ] || [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"
      case "$2" in
        --create) 
            COL_CREATE_NAME=$3
            COL_CREATE_NUMSHARDS=1
            shift 3
            while test $# -gt 0 ; do
              case "$1" in
                -s)
                  [ $# -gt 1 ] || usage "Error: collection --create name $1 requires an argument"
                  COL_CREATE_NUMSHARDS="$2"
                  shift 2
                  ;;
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --create name $1 requires an argument"
                  COL_CREATE_CONFNAME="$2"
                  shift 2
                  ;;
                -r)
                  [ $# -gt 1 ] || usage "Error: collection --create name $1 requires an argument"
                  COL_CREATE_REPL="$2"
                  shift 2
                  ;;
                -m)
                  [ $# -gt 1 ] || usage "Error: collection --create name $1 requires an argument"
                  COL_CREATE_MAXSHARDS="$2"
                  shift 2
                  ;;
                -n)
                  [ $# -gt 1 ] || usage "Error: collection --create name $1 requires an argument"
                  COL_CREATE_NODESET="$2"
                  shift 2
                  ;;
                -a)
                  COL_AUTO_ADD_REPLICAS="true"
                  shift 1
                  ;;
                 *)
                  break
                  ;;
              esac
            done

            COL_CREATE_CALL="${COL_CREATE_CALL}&name=${COL_CREATE_NAME}"
            if [ "$COL_CREATE_NUMSHARDS" -gt 0 ] ; then
              COL_CREATE_CALL="${COL_CREATE_CALL}&numShards=${COL_CREATE_NUMSHARDS}"
            else
              usage "Error: collection --create name needs to have more than 0 shards specified"
            fi
            [ -n "$COL_CREATE_CONFNAME" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&collection.configName=${COL_CREATE_CONFNAME}"
            [ -n "$COL_CREATE_REPL" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&replicationFactor=${COL_CREATE_REPL}"
            [ -n "$COL_CREATE_MAXSHARDS" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&maxShardsPerNode=${COL_CREATE_MAXSHARDS}"
            [ -n "$COL_CREATE_NODESET" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&createNodeSet=${COL_CREATE_NODESET}"
            [ -n "$COL_AUTO_ADD_REPLICAS" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&autoAddReplicas=true"

            eval $SOLR_ADMIN_API_CMD "'/admin/collections?action=CREATE${COL_CREATE_CALL}'"

            shift 4
            ;;
        --delete|--reload)
            COL_ACTION=`echo $2 | tr '[a-z]-' '[A-Z] '`
            eval $SOLR_ADMIN_API_CMD "'/admin/collections?action=`echo $COL_ACTION`&name=$3'"
            shift 3
            ;;
        --deletedocs)
            eval $SOLR_ADMIN_API_CMD "'/$3/update?commit=true'" -H "'Content-Type: text/xml'" "--data-binary '<delete><query>*:*</query></delete>'"
            shift 3
            ;;
        --stat)
            get_solr_state | sed -ne '/\/collections\//s#^.*/collections/##p' | sed -ne '/election\//s###p' | grep "$3/"
            shift 3
            ;;
        --list)
            get_solr_state | sed -ne '/\/collections\/[^\/]*$/s#^.*/collections/##p'
            shift 2
            ;;
        --create-snapshot)
            SNAPSHOT_NAME="$3"
            shift 3
            while test $# -gt 0 ; do
              case "$1" in
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --create-snapshot snapshotName $1 requires an argument"
                  COLLECTION_NAME="$2"
                  shift 2
                  ;;
                 *)
                  break
                  ;;
              esac
            done

            if [ -z "$SNAPSHOT_NAME" ] ; then
              usage "Error: collection --create-snapshot needs to have a snapshot name specified"
            fi
            if [ -z "$COLLECTION_NAME" ]; then
              usage "Error: collection --create-snapshot needs to have a collection name specified with -c parameter"
            fi

            eval 'run_solr_snapshot_tool --create ${SNAPSHOT_NAME} -c ${COLLECTION_NAME} -z ${SOLR_ZK_ENSEMBLE}' || die "Error: Unable to create snapshot"
            shift 4
            ;;
        --delete-snapshot)
            SNAPSHOT_NAME="$3"
            shift 3
            while test $# -gt 0 ; do
              case "$1" in
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --delete-snapshot snapshotName $1 requires an argument"
                  COLLECTION_NAME="$2"
                  shift 2
                  ;;
                 *)
                  break
                  ;;
              esac
            done

            if [ -z "$SNAPSHOT_NAME" ] ; then
              usage "Error: collection --delete-snapshot needs to have a snapshot name specified"
            fi
            if [ -z "$COLLECTION_NAME" ]; then
              usage "Error: collection --delete-snapshot needs to have a collection name specified with -c parameter"
            fi

            eval 'run_solr_snapshot_tool --delete ${SNAPSHOT_NAME} -c ${COLLECTION_NAME} -z ${SOLR_ZK_ENSEMBLE}' || die "Error: Unable to delete snapshot"
            shift 4
            ;;
        --describe-snapshot)
            SNAPSHOT_NAME="$3"
            shift 3
            while test $# -gt 0 ; do
              case "$1" in
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --describe-snapshot snapshotName $1 requires an argument"
                  COLLECTION_NAME="$2"
                  shift 2
                  ;;
                 *)
                  break
                  ;;
              esac
            done

            if [ -z "$SNAPSHOT_NAME" ] ; then
              usage "Error: collection --describe-snapshot needs to have a snapshot name specified"
            fi
            if [ -z "$COLLECTION_NAME" ]; then
              usage "Error: collection --describe-snapshot needs to have a collection name specified with -c parameter"
            fi

            eval 'run_solr_snapshot_tool --describe ${SNAPSHOT_NAME} -c ${COLLECTION_NAME} -z ${SOLR_ZK_ENSEMBLE}' || die "Error: Couldn't fetch snapshot details"
            shift 4
            ;;
        --list-snapshots)
            COLLECTION_NAME="$3"

            if [ -z "$COLLECTION_NAME" ]; then
              usage "Error: collection --list-snapshots needs to have a collection name specified"
            fi

            eval 'run_solr_snapshot_tool --list -c ${COLLECTION_NAME} -z ${SOLR_ZK_ENSEMBLE}'  || die "Error: Couldn't list snapshots"
            shift 3
            ;;
        --prepare-snapshot-export)
            SNAPSHOT_NAME="$3"
            shift 3

            while test $# -gt 0 ; do
              case "$1" in
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --prepare-snapshot-export snapshotName $1 requires an argument"
                  COLLECTION_NAME="$2"
                  shift 2
                  ;;
                -d)
                  [ $# -gt 1 ] || usage "Error: collection --prepare-snapshot-export snapshotName $1 requires an argument"
                  DEST_DIR_PATH="$2"
                  shift 2
                  ;;
                -p)
                  [ $# -gt 1 ] || usage "Error: collection --prepare-snapshot-export snapshotName $1 requires an argument"
                  FS_PATH_PREFIX="$2"
                  shift 2
                  ;;
                *)
                  break
                  ;;
              esac
            done

            if [ -z "${SNAPSHOT_NAME}" ] ; then
              usage "Error: collection --prepare-snapshot-export needs to have a snapshot name specified"
            fi
            if [ -z "${COLLECTION_NAME}" ]; then
              usage "Error: collection --prepare-snapshot-export needs to have a collection name specified"
            fi
            if [ -z "${DEST_DIR_PATH}" ]; then
              usage "Error: collection --prepare-snapshot-export needs to have a destination directory path specified"
            fi

            EXPORT_PREPARE_EXTRA_OPTS=""
            if [ -n "${FS_PATH_PREFIX}" ]; then
              EXPORT_PREPARE_EXTRA_OPTS=" -p ${FS_PATH_PREFIX}"
            fi

            eval 'run_solr_snapshot_tool --prepare-snapshot-export ${SNAPSHOT_NAME}  -c ${COLLECTION_NAME} -z ${SOLR_ZK_ENSEMBLE} -d ${DEST_DIR_PATH} ${EXPORT_PREPARE_EXTRA_OPTS}' || die "Error: Unable to prepare for snapshot export"
            ;;
        --export-snapshot)
            SNAPSHOT_NAME="$3"
            shift 3

            while test $# -gt 0 ; do
              case "$1" in
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --prepare-snapshot-export snapshotName $1 requires an argument"
                  COLLECTION_NAME="$2"
                  shift 2
                  ;;
                -s)
                  [ $# -gt 1 ] || usage "Error: collection --snapshot-export ${SNAPSHOT_NAME} $1 requires an argument"
                  SOURCE_DIR_PATH="$2"
                  shift 2
                  ;;
                -d)
                  [ $# -gt 1 ] || usage "Error: collection --snapshot-export ${SNAPSHOT_NAME} $1 requires an argument"
                  DEST_DIR_PATH="$2"
                  shift 2
                  ;;
                *)
                  break
                  ;;
              esac
            done

            if [ -z "${SNAPSHOT_NAME}" ] ; then
              usage "Error: collection --snapshot-export needs to have a snapshot name specified"
            fi
            if [ -z "${DEST_DIR_PATH}" ]; then
              usage "Error: collection --snapshot-export needs to have a destination directory path specified"
            fi
            if [ -n "${COLLECTION_NAME}" ] && [ -n "${SOURCE_DIR_PATH}" ]; then
              echo "The -c and -s options can not be specified together"
              exit 1
            fi
            if [ -z "${COLLECTION_NAME}" ] && [ -z "${SOURCE_DIR_PATH}" ]; then
              echo "At least one of options (-c or -s) must be specified"
              exit 1
            fi

            EXPORT_SNAPSHOT_OPTS=""
            if [ -n "${COLLECTION_NAME}" ]; then
              EXPORT_SNAPSHOT_OPTS="${EXPORT_SNAPSHOT_OPTS} -c ${COLLECTION_NAME}"
            fi
            if [ -n "${SOURCE_DIR_PATH}" ]; then
              EXPORT_SNAPSHOT_OPTS="${EXPORT_SNAPSHOT_OPTS} -s ${SOURCE_DIR_PATH}"
            fi

            eval 'run_solr_snapshot_tool --export-snapshot ${SNAPSHOT_NAME} -z ${SOLR_ZK_ENSEMBLE} -d ${DEST_DIR_PATH} ${EXPORT_SNAPSHOT_OPTS}' || die "Error: Unable to export snapshot"
            ;;
        --restore)
            COL_RESTORE_NAME=$3
            shift 3
            while test $# -gt 0 ; do
              case "$1" in
                -c)
                  [ $# -gt 1 ] || usage "Error: collection --restore name $1 requires an argument"
                  COL_RESTORE_CONFNAME="$2"
                  shift 2
                  ;;
                -r)
                  [ $# -gt 1 ] || usage "Error: collection --restore name $1 requires an argument"
                  COL_RESTORE_REPL="$2"
                  shift 2
                  ;;
                -m)
                  [ $# -gt 1 ] || usage "Error: collection --restore name $1 requires an argument"
                  COL_RESTORE_MAXSHARDS="$2"
                  shift 2
                  ;;
                -a)
                  COL_AUTO_ADD_REPLICAS="true"
                  shift 1
                  ;;
                -l)
                  [ $# -gt 1 ] || usage "Error: collection --restore name $1 requires an argument"
                  COL_RESTORE_LOCATION="$2"
                  shift 2
                  ;;
                 -b)
                  [ $# -gt 1 ] || usage "Error: collection --restore name $1 requires an argument"
                  COL_BACKUP_NAME="$2"
                  shift 2
                  ;;
                 -i)
                  [ $# -gt 1 ] || usage "Error: collection --restore name $1 requires an argument"
                  ASYNC_REQUEST_ID="$2"
                  shift 2
                  ;;
                 *)
                  break
                  ;;
              esac
            done

            if [ -z "${COL_RESTORE_LOCATION}" ]; then
              usage "Error: collection --restore needs to specify the backup location"
            fi
            if [ -z "${COL_BACKUP_NAME}" ]; then
              usage "Error: collection --restore needs specify the backup name"
            fi
            if [ -z "${ASYNC_REQUEST_ID}" ]; then
              usage "Error: collection --restore needs specify unique request id for async execution"
            fi

            COL_RESTORE_CALL="${COL_RESTORE_CALL}&collection=${COL_RESTORE_NAME}&name=${COL_BACKUP_NAME}&location=${COL_RESTORE_LOCATION}&async=${ASYNC_REQUEST_ID}"
            [ -n "$COL_RESTORE_CONFNAME" ] && COL_RESTORE_CALL="${COL_RESTORE_CALL}&collection.configName=${COL_RESTORE_CONFNAME}"
            [ -n "$COL_RESTORE_REPL" ] && COL_RESTORE_CALL="${COL_RESTORE_CALL}&replicationFactor=${COL_RESTORE_REPL}"
            [ -n "$COL_RESTORE_MAXSHARDS" ] && COL_RESTORE_CALL="${COL_RESTORE_CALL}&maxShardsPerNode=${COL_RESTORE_MAXSHARDS}"
            [ -n "$COL_AUTO_ADD_REPLICAS" ] && COL_RESTORE_CALL="${COL_RESTORE_CALL}&autoAddReplicas=true"

            eval $SOLR_ADMIN_API_CMD "'/admin/collections?action=RESTORE${COL_RESTORE_CALL}'" || die "Error: Unable to restore the collection"

            shift 4
            ;;
        --request-status)
            ASYNC_REQUEST_ID="$3"
            shift 3

            if [ -z "${ASYNC_REQUEST_ID}" ] ; then
              usage "Error: collection --request-status needs to have a request id specified during async invocation"
            fi

            eval $SOLR_ADMIN_API_CMD "'/admin/collections?action=REQUESTSTATUS&requestid=${ASYNC_REQUEST_ID}'" || die "Error: Unable to fetch the status for the request"
            ;;
        *)  
            shift 1
            ;;
      esac
      ;;

    core)
      [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"
      case "$2" in
        --create)
          CORE_CREATE_NAME="$3"
          shift 3
          while test $# -gt 0 ; do
            case "$1" in
              -p)
                [ $# -gt 1 ] || usage "Error: core --create name $1 requires an argument of key=value"
                CORE_KV_PAIRS="${CORE_KV_PAIRS}&${2}"
                shift 2
                ;;
               *)
                break
                ;;
            esac
          done
          [ -n "$CORE_KV_PAIRS" ] || CORE_KV_PAIRS="&instanceDir=${CORE_CREATE_NAME}"
 
          eval $SOLR_ADMIN_API_CMD "'/admin/cores?action=CREATE&name=${CORE_CREATE_NAME}${CORE_KV_PAIRS}'"
          ;;
        --reload|--unload|--status)
          CORE_ACTION=`echo $2 | tr '[a-z]-' '[A-Z] '`
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $CORE_ACTION"
          eval $SOLR_ADMIN_API_CMD "'/admin/cores?action=`echo $CORE_ACTION`&core=$3'"
          shift 3
          ;;
        *)  
          shift 1
          ;;
      esac
      ;;

    cluster)
      [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"
      case "$2" in
        --get-solrxml)
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $2"
          [ ! -e "$3" ] || die "$3 already exists"
          > "$3" || die "unable to create file $3"
          eval $SOLR_ADMIN_ZK_CMD -cmd getfile /solr.xml $3  || die "Error: can't get solr.xml from ZK"
          shift 3
          ;;
        --put-solrxml)
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $2"
          [ -f "$3" ] || die "$3 is not a file"
          eval $SOLR_ADMIN_ZK_CMD -cmd clear /solr.xml || die "Error: failed to clear solr.xml in ZK before put"
          eval $SOLR_ADMIN_ZK_CMD -cmd putfile /solr.xml "$3" || die "Error: can't put solr.xml to ZK"
          shift 3
          ;;
        --set-property)
          if [ $# -eq 4 ]; then
            eval $SOLR_ADMIN_ZK_CMD -cmd CLUSTERPROP -name ${3} -val ${4} || die "Error: Unable to set the cluster property in ZK"
            shift 4
          else
            usage "Error: incorrect specification of arguments for $2"
          fi
          ;;
        --remove-property)
          if [ $# -eq 3 ]; then
            eval $SOLR_ADMIN_ZK_CMD -cmd CLUSTERPROP -name ${3} || die "Error: Unable to remove the cluster property in ZK"
            shift 3
          else
            usage "Error: incorrect specification of arguments for $2"
          fi
          ;;
        --get-clusterstate)
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $2"
          [ ! -e "$3" ] || die "$3 already exists"
          > "$3" || die "unable to create file $3"
          eval $SOLR_ADMIN_ZK_CMD -cmd getfile /clusterstate.json $3  || die "Error: can't get clusterstate.json from ZK or clusterstate.json is empty"
          shift 3
          ;;
        *)
          shift 1
          ;;
        esac
        ;;

    sentry)
      [ $# -gt 1 ] || usage "Error: incorrect specification of arguments for $1"
      # Put hadoop native libs in java.library.path to avoid a WARN in logs
      SENTRYCLI_JVM_FLAGS="${SENTRYCLI_JVM_FLAGS} -Djava.library.path=${HADOOP_HOME}/lib/native"
      export LOG4J_PROPS=${SOLR_CONF_DIR}/log4j.properties
      # NOTE: Instead of doing the normal "eval" method for invoking the sentrycli script
      # we put the entire command here so it is only evaled once (on the user's command line).
      # This is because of privilege specification, e.g. 'collection=collection1->action=UPDATE'
      # works correctly, but needs to be escaped if evaled.
      SENTRY_CLI_CMD="${SOLR_HOME}/bin/sentrycli.sh org.apache.sentry.provider.db.generic.tools.SentryShellSolr -conf ${SENTRY_CONF_DIR}/sentry-site.xml"
      SENTRY_CONFIGTOOL_CMD="${SOLR_HOME}/bin/sentrycli.sh org.apache.sentry.provider.db.generic.tools.SentryConfigToolSolr -conf ${SENTRY_CONF_DIR}/sentry-site.xml"
      case "$2" in
        --create-role)
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --create_role -r $3 || die ""
          shift 3
          ;;
        --drop-role)
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --drop_role -r $3 || die ""
          shift 3
          ;;
        --add-role-group)
          [ $# -eq 4 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --add_role_group -r $3 -g $4 || die ""
          shift 4
          ;;
        --delete-role-group)
          [ $# -eq 4 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --delete_role_group -r $3 -g $4 || die ""
          shift 4
          ;;
        --list-roles)
          [ $# -eq 2 ] || [ $# -eq 4 -a "$3" == "-g" ] || usage "Error: incorrect specification of arguments for $2"
          if [ $# -eq 4 ]; then
            SENTRY_LIST_ROLE_GROUP="$3 $4"
            shift 2
          fi
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --list_role $SENTRY_LIST_ROLE_GROUP || die ""
          shift 2
          ;;
        --grant-privilege)
          [ $# -eq 4 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --grant_privilege_role -r $3 -p $4 || die ""
          shift 4
          ;;
        --revoke-privilege)
          [ $# -eq 4 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --revoke_privilege_role -r $3 -p $4 || die ""
          shift 4
          ;;
        --list-privileges)
          [ $# -eq 3 ] || usage "Error: incorrect specification of arguments for $2"
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CLI_CMD} --list_privilege -r $3 || die ""
          shift 3
          ;;
        --convert-policy-file)
          [ $# -eq 3 ] || [ $# -eq  4 ] || usage "Error: incorrect specification of arguments for $2"
          importpolicy="-i"
          if [ $# -eq 4 -a "$4" = "-dry-run" ] ; then
            importpolicy=
          fi
          SENTRYCLI_JVM_FLAGS=${SENTRYCLI_JVM_FLAGS} ${SENTRY_CONFIGTOOL_CMD} -p $3 -v -c $importpolicy || die ""
          if [ -z $importpolicy ] ; then
            shift 4
          else
            shift 3
          fi
          ;;
        *)
          shift 1
          ;;
        esac
        ;;

    *)
      usage "Error: unrecognized command $1"
      ;;
  esac
done

# If none of the above commands ended up calling die -- we're OK
exit 0
