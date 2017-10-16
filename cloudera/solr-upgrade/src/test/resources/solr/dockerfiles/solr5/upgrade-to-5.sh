#!/usr/bin/env bash
libdir=/opt/solr-5.5.5/server/solr-webapp/webapp/WEB-INF/lib
java -cp $libdir/lucene-core-5.5.5.jar:$libdir/lucene-backward-codecs-6.0.0.jar org.apache.lucene.index.IndexUpgrader -delete-prior-commits -verbose $1
