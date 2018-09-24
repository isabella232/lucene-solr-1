#!/usr/bin/env bash

JVM="java"
scriptDir=$(dirname "$0")
solrLibPath="${SOLR_LIB_PATH:-$scriptDir/../../solr-webapp/webapp/WEB-INF/lib/*:$scriptDir/../../lib/ext/*}"

if [ -n "$SOLR_AUTHORIZATION_SUPERUSER" ] ; then
  ZKCLI_JVM_FLAGS="-Dsolr.authorization.superuser=${SOLR_AUTHORIZATION_SUPERUSER} ${ZKCLI_JVM_FLAGS}"
fi

if [ -n "$ZK_SASL_CLIENT_USERNAME" ] ; then
  ZKCLI_JVM_FLAGS="-Dzookeeper.sasl.client.username=${ZK_SASL_CLIENT_USERNAME} ${ZKCLI_JVM_FLAGS}"
fi

if [ -n "$ZKCLI_TMPDIR" ] ; then
  ZKCLI_JVM_FLAGS="-Djava.io.tmpdir=${ZKCLI_TMPDIR} ${ZKCLI_JVM_FLAGS}"
fi

if [ -n "$LOG4J_PROPS" ]; then
  log4j_config="file:${LOG4J_PROPS}"
else
  log4j_config="file:${scriptDir}/log42.xml"
fi

PATH="${JAVA_HOME}/bin:${PATH}" ${JVM} ${ZKCLI_JVM_FLAGS} -Dlog4j.configuration=${log4j_config} \
  -classpath "${solrLibPath}" org.apache.solr.cloud.ZkSyncTool
