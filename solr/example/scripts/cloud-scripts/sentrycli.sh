#!/usr/bin/env bash

# CLOUDERA-BUILD.  Support for sentry commands via solrctl.  This could live
# in a cloudera-specific directory, however since it is meant to match up
# closely with zkcli.sh, it makes sense to co-locate it with that script.

# You can override pass the following parameters to this script:
#

JVM="java"

# Find location of this script

sdir="`dirname \"$0\"`"

PATH=$JAVA_HOME/bin:$PATH $JVM $SENTRYCLI_JVM_FLAGS -Dlog4j.configuration=file:$sdir/log4j.properties -classpath "$sdir/../solr-webapp/webapp/WEB-INF/lib/*:$sdir/../lib/ext/*" org.apache.sentry.provider.db.generic.tools.SentryShellSolr ${1+"$@"}

