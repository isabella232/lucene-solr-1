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

usage() {
  [ $# -eq 0 ] || echo "$@"
  echo "
usage: $0 [options] command [command-arg] [command [command-arg]] ...

Options:
    --solr solr_uri
    --zk   zk_ensemble
    --help
    --quiet

Commands:
    init        [--force]

    coreconfig  [--generate path]
                [--create name path]
                [--get name path]
                [--delete name]
                [--list]

    collection  [--create name -s <numShards>
                              [-c <collection.configName>]
                              [-r <replicationFactor>]
                              [-m <maxShardsPerNode>]
                              [-n <createNodeSet>]]
                [--delete name]
                [--reload name]
                [--deletedocs name]

    core        [--create name [-p name=value]...]
                [--reload name]
                [--unload name]
                [--status name]

    nodeconfig  [--recover]
  "
  exit 1
}

die() {
  echo "$1"
  exit 1
}

# FIXME: this is here only for testing purposes
local_coreconfig() {
  case "$2" in
    list)
      ls -d /var/lib/solr/*/conf 2>/dev/null | sed -e 's#var/lib/solr#configs#'
      ;;
    clear)
      sudo -u solr rm -rf /var/lib/solr/`basename $3`/* 2>/dev/null
      ;;
    upconfig)
      # the following trick gets us around permission issues
      rm -rf /tmp/solr_conf.$$
      cp -r $4 /tmp/solr_conf.$$
      chmod o+r -R /tmp/solr_conf.$$
      sudo -u solr bash -c "mkdir /var/lib/solr/$6 2>/dev/null ; cp -r /tmp/solr_conf.$$ /var/lib/solr/$6/conf"
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
  local WEB_OUT=`curl -i --retry 5 -s -L -k "$@" | sed -e 's#>#>\n#g'`

  if [ $? -eq 0 ] && (echo "$WEB_OUT" | grep -q 'HTTP/.*200.*OK') ; then
    echo "$WEB_OUT" | egrep -q '<lst name="(failure|exception|error)">' || return 0
  fi

  die "Error: A call to SolrCloud WEB APIs failed: $WEB_OUT"
}

if [ -e /etc/default/solr ] ; then
  . /etc/default/solr
else
  SOLR_PORT=8983
fi

SOLR_ADMIN_URI="http://localhost:$SOLR_PORT/solr"
SOLR_ADMIN_CHAT=echo
SOLR_ADMIN_API_CMD='solr_webapi'

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
    *)
      break
      ;;
  esac
done

if [ -z "$SOLR_ZK_ENSEMBLE" ] ; then
  SOLR_ADMIN_ZK_CMD="local_coreconfig"
  cat >&2 <<-__EOT__
	Warning: Non-SolrCloud mode has been completely deprecated
	Please configure SolrCloud via SOLR_ZK_ENSEMBLE setting in 
	/etc/default/solr
	If you running remotely, please use --zk zk_ensemble.
	__EOT__
else
  SOLR_ADMIN_ZK_CMD='/usr/lib/solr/bin/zkcli.sh -zkhost $SOLR_ZK_ENSEMBLE 2>/dev/null'
fi

# Now start parsing commands -- there has to be at least one!
[ $# -gt 0 ] || usage 
while test $# != 0 ; do
  case "$1" in 
    init)
      if [ "$2" == "--force" ] ; then
        shift 1
      else
        ZK_DUMP=`(eval $SOLR_ADMIN_ZK_CMD -cmd list) | grep -v '^/ '`
        LIVE_NODES=`echo "$ZK_DUMP" | sed -ne 's#/live_nodes/##p'`

        if [ -n "$LIVE_NODES" ] ; then
          die "Warning: It appears you have live SolrCloud nodes running: `printf '\n%s\nPlease shut them down.' \"${LIVE_NODES}\"`"
        elif [ -n "$ZK_DUMP" ] ; then
          die "Warning: Solr appears to be initialized (use --force to override)"
        fi
      fi

      eval $SOLR_ADMIN_ZK_CMD -cmd makepath / > /dev/null 2>&1 || : 
      eval $SOLR_ADMIN_ZK_CMD -cmd clear /    || die "Error: failed to initialize Solr"

      shift 1
      ;;

    nodeconfig)
      if [ "$2" == "--recover" ] ; then
        CORES=`eval $SOLR_ADMIN_ZK_CMD -cmd getcollections -hostname $HOSTNAME`
        shift 1
      fi

      # FIXME: this is slightly clunky -- we basically peg the configuration to 
      #        SolCloud/non SolrCloud in order to avoid confusion later on
      if [ -n "$SOLR_ZK_ENSEMBLE" ] ; then
        touch /var/lib/solr/solr.cloud.ini
      fi
      [ -e /var/lib/solr/solr.xml ] && mv /var/lib/solr/solr.xml `mktemp -u /var/lib/solr/solr.xml.XXXXXXXXXX`
      cat > /var/lib/solr/solr.xml <<-__EOT__
	<?xml version="1.0" encoding="UTF-8" ?>
	<solr persistent="true">
	  <cores defaultCoreName="default" host="\${solr.host:}" adminPath="/admin/cores" zkClientTimeout="\${zkClientTimeout:15000}" hostPort="\${solr.port:}" hostContext="solr">
	  $CORES
	  </cores>
	</solr>
	__EOT__

      shift 1
      ;;

    coreconfig)
      [ $# -gt 1 ] || usage "Error: incorrect specification of arguments for $1"
      case "$2" in
        --create) 
            [ $# -gt 3 ] || usage "Error: incorrect specification of arguments for $1 $2" 
            [ -e $4/solrconfig.xml -a -e $4/schema.xml ] || die "Error: $4 must be a directory with at least solrconfig.xml and schema.xml"
            # FIXME: perhaps we have to warn user if configs already exist in ZK

            $SOLR_ADMIN_CHAT "Uploading configs from $4 to $SOLR_ZK_ENSEMBLE. This may take up to a minute."
            eval $SOLR_ADMIN_ZK_CMD -cmd upconfig -confdir $4 -confname $3 2>/dev/null || die "Error: can't upload configuration"
            shift 4
            ;;
        --get)
            [ $# -gt 3 ] || usage "Error: incorrect specification of arguments for $1 $2"

            $SOLR_ADMIN_CHAT "Downloading configs from $SOLR_ZK_ENSEMBLE to $4. This may take up to a minute."
            eval $SOLR_ADMIN_ZK_CMD -cmd downconfig -confdir $4 -confname $3 2>/dev/null || die "Error: can't download configuration"
            shift 4
            ;;
        --delete)
            [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"
            
            eval $SOLR_ADMIN_ZK_CMD -cmd clear /configs/$3 2>/dev/null || die "Error: can't delete configuration"
            shift 3
            ;;
        --generate)
            [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"

            mkdir -p $3 > /dev/null 2>&1
            [ -d $3 ] || usage "Error: $3 has to be a directory"
            cp -r /usr/lib/solr/coreconfig-template/* $3
            shift 3
            ;;
        --list)
            eval $SOLR_ADMIN_ZK_CMD -cmd list 2>/dev/null | sed -n -e '/\/configs\//s#^.*/configs/\([^/]*\)/.*$#\1#p' | sort -u
            shift 2
            ;;
        *)  
            shift 1
            ;;
      esac
      ;;

    collection)
      [ $# -gt 2 ] || usage "Error: incorrect specification of arguments for $1"
      case "$2" in
        --create) 
            [ $# -gt 3 ] || usage "Error: incorrect specification of arguments for $1 $2"
            COL_CREATE_NAME=$3
            COL_CREATE_NUMSHARDS=0
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
                 *)
                  break
                  ;;
              esac
            done

            COL_CREATE_CALL="&name=${COL_CREATE_NAME}"
            if [ "$COL_CREATE_NUMSHARDS" -gt 0 ] ; then
              COL_CREATE_CALL="${COL_CREATE_CALL}&numShards=${COL_CREATE_NUMSHARDS}"
            else
              usage "Error: collection --create name needs to have more than 0 shards specified"
            fi
            [ -n "$COL_CREATE_CONFNAME" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&collection.configName=${COL_CREATE_CONFNAME}"
            [ -n "$COL_CREATE_REPL" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&replicationFactor=${COL_CREATE_REPL}"
            [ -n "$COL_CREATE_MAXSHARDS" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&maxShardsPerNode=${COL_CREATE_MAXSHARDS}"
            [ -n "$COL_CREATE_NODESET" ] && COL_CREATE_CALL="${COL_CREATE_CALL}&createNodeSet=${COL_CREATE_NODESET}"
            
            eval $SOLR_ADMIN_API_CMD "'$SOLR_ADMIN_URI/admin/collections?action=CREATE${COL_CREATE_CALL}'"

            shift 4
            ;;
        --delete|--reload)
            COL_ACTION=`echo $2 | tr '[a-z]-' '[A-Z] '`
            eval $SOLR_ADMIN_API_CMD "'$SOLR_ADMIN_URI/admin/collections?action=`echo $COL_ACTION`&name=$3'"
            shift 3
            ;;
        --deletedocs)
            eval $SOLR_ADMIN_API_CMD "'$SOLR_ADMIN_URI/$3/update?commit=true'" -H "'Content-Type: text/xml'" "--data-binary '<delete><query>*:*</query></delete>'"
            shift 3
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
 
          eval $SOLR_ADMIN_API_CMD "'$SOLR_ADMIN_URI/admin/cores?action=CREATE&name=${CORE_CREATE_NAME}${CORE_KV_PAIRS}'"
          ;;
        --reload|--unload|--status)
          CORE_ACTION=`echo $2 | tr '[a-z]-' '[A-Z] '`
          eval $SOLR_ADMIN_API_CMD "'$SOLR_ADMIN_URI/admin/cores?action=`echo $CORE_ACTION`&core=$3'"
          shift 3
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
