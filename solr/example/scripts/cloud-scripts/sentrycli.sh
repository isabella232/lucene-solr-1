#!/usr/bin/env bash

# CLOUDERA-BUILD.  Support for sentry commands via solrctl.  This could live
# in a cloudera-specific directory, however since it is meant to match up
# closely with zkcli.sh, it makes sense to co-locate it with that script.

# You can override pass the following parameters to this script:
#

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"

if [ -n "$LOG4J_PROPS" ]; then
  log4j_config="file:$LOG4J_PROPS"
else
  log4j_config="file:$sdir/log4j.properties"
fi

PATH=$JAVA_HOME/bin:$PATH $JVM $SENTRYCLI_JVM_FLAGS -Dlog4j.configuration=$log4j_config -classpath "$sdir/../webapps/solr/WEB-INF/lib/*:$sdir/../lib/ext/*" ${1+"$@"}

