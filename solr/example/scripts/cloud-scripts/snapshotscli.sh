#!/usr/bin/env bash

set -e

#Make sure to cleanup the temporary files.
scratch=$(mktemp -d -t solrsnaps.XXXXXXXXXX)
function finish {
  rm -rf "$scratch"
}
trap finish EXIT

usage() {
  cat << __EOF
Usage: $(basename "${BASH_SOURCE}")
    [ snapshotscli.sh [-z solrZkEnsemble] [-c collection_name] [-s snapshot_name] [-d destination_path] [-t hdfs_tmp_dir] ]
    -z | solr_zk_ensemble
    -c | collection_name
    -s | snapshot_name
    -d | destination_path
    -t | hdfs_tmp_dir
__EOF
}

build_export_copylisting() {
  PATH=$JAVA_HOME/bin:$PATH $JVM $ZKCLI_JVM_FLAGS -Dlog4j.configuration=$log4j_config \
  -classpath "$scriptDir/../webapps/solr/WEB-INF/lib/*:$scriptDir/../lib/ext/*" \
  org.apache.solr.core.snapshots.SolrSnapshotExportTool ${solrZkEnsemble} ${collectionName} ${snapshotName} ${scratch} ${hdfsTempDir} 2> /dev/null
}

copy_using_distcp() {
  if hdfs dfs -test -d ${hdfsTempDir}/copylistings; then
    hdfs dfs -rm -r -f -skipTrash "${hdfsTempDir}/copylistings/*" > /dev/null
  else
    hdfs dfs -mkdir -p "${hdfsTempDir}/copylistings" > /dev/null
  fi

  find "${scratch}" -type f -printf "%f\n" | while read shardId; do
    oPath="${destPath}/${snapshotName}/snapshot.${shardId}"

    echo "Copying the index files for $shardId to ${oPath}"
    hdfs dfs -copyFromLocal "${scratch}/${shardId}" "${hdfsTempDir}/copylistings/${shardId}" > /dev/null
    hadoop distcp -f "${hdfsTempDir}/copylistings/${shardId}" "${oPath}" > /dev/null
  done

  echo "Copying the collection meta-data to ${destPath}/${snapshotName}"
  hadoop distcp "${hdfsTempDir}/${snapshotName}/*" "${destPath}/${snapshotName}/" > /dev/null
}

scriptDir="`dirname \"$0\"`"
if [ -n "$LOG4J_PROPS" ]; then
  log4j_config="file:$LOG4J_PROPS"
else
  log4j_config="file:$sdir/log4j.properties"
fi

JVM="java"
solrZkEnsemble=
collectionName=
snapshotName=
destPath=
hdfsTempDir=
while getopts ":z:c:s:d:t:" o; do
  case "${o}" in
    z)
      solrZkEnsemble=${OPTARG}
      ;;
    c)
      collectionName=${OPTARG}
      ;;
    s)
      snapshotName=${OPTARG}
      ;;
    d)
      destPath=${OPTARG}
      ;;
    t)
      hdfsTempDir=${OPTARG}
      ;;
    *)
      usage 1>&2
      exit 1
  esac
done

if [ -z "${solrZkEnsemble}" ]; then
  echo "Please specify Solr Zookeeper ensemble"
  usage 1>&2
  exit 1
fi

if [ -z "${collectionName}" ]; then
  echo "Please specify the collection name"
  usage 1>&2
  exit 1
fi

if [ -z "${snapshotName}" ]; then
  echo "Please specify the snapshot name"
  usage 1>&2
  exit 1
fi

if [ -z "${destPath}" ]; then
  echo "Please specify the destination path (where the snapshot is to be exported)"
  usage 1>&2
  exit 1
fi

if [ -z "${hdfsTempDir}" ]; then
  echo "Please specify a directory on HDFS where the temporary files should be stored during the export operation"
  usage 1>&2
  exit 1
fi

if hdfs dfs -test -d ${hdfsTempDir} ; then
  hdfs dfs -rm -r -f -skipTrash "${hdfsTempDir}/*" > /dev/null
  build_export_copylisting
  if [ $? -eq 0 ]; then
    copy_using_distcp
    echo "Done. GoodBye!"
    exit 0
  fi
else
  echo "Directory ${hdfsTempDir} does not exist."
  exit 1
fi

