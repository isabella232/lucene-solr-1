#!/usr/bin/env bash

JVM="java"
scriptDir=$(dirname "$0")
solrLibPath="${SOLR_LIB_PATH:-$scriptDir/../../solr-webapp/webapp/WEB-INF/lib/*:$scriptDir/../../lib/ext/*}"

if [ -n "$LOG4J_PROPS" ]; then
  log4j_config="file:${LOG4J_PROPS}"
else
  log4j_config="file:${scriptDir}/log4j.properties"
fi

PATH="${JAVA_HOME}/bin:${PATH}" ${JVM} ${ZKCLI_JVM_FLAGS} -Dlog4j.configuration=${log4j_config} \
  -classpath "${solrLibPath}" org.apache.solr.cloud.ZkSyncTool
