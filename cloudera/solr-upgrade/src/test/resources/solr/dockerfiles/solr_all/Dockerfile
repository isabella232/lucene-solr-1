FROM anapsix/alpine-java:8_jdk_unlimited

ARG docker_source_version

ENV target_version solr-7.4.0-SNAPSHOT
ENV source_version ${docker_source_version}


#You need to build this tgz using 'ant package' in solr/ directory.
COPY ${target_version}.tgz /
RUN cd / && \
  tar xvf ${target_version}.tgz && \
  rm -f ${target_version}.tgz && \
  mv ${target_version} solr-to


ADD http://archive.cloudera.com/cdh5/cdh/5/${source_version}.tar.gz /
RUN cd / && \
  tar xvf ${source_version}.tar.gz && \
  rm -f ${source_version}.tar.gz && \
  mv ${source_version} solr-from

RUN mkdir /solr-from/example/solr/lib && \
  cp /solr-from/contrib/depends-sentry-libs/lib/* /solr-from/example/solr/lib && \
  cp /solr-from/contrib/analysis-extras/lib/*.jar /solr-from/example/solr/lib && \
  cp /solr-from/contrib/analysis-extras/lucene-libs/*.jar /solr-from/example/solr/lib && \
  jar tf /solr-from/example/webapps/solr.war | grep analyzers-common |xargs jar xvf /solr-from/example/webapps/solr.war && \
  cp WEB-INF/lib/lucene-analyzers-common* /solr-from/example/solr/lib