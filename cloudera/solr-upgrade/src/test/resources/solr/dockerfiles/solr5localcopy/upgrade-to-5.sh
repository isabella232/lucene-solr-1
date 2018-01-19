#!/usr/bin/env bash
libdir=/opt/solr-5.6.0-SNAPSHOT/server/solr-webapp/webapp/WEB-INF/lib
java -cp $libdir/lucene-core-5.6.0-SNAPSHOT.jar:$libdir/lucene-backward-codecs-5.6.0-SNAPSHOT.jar org.apache.lucene.index.IndexUpgrader -delete-prior-commits -verbose $1
