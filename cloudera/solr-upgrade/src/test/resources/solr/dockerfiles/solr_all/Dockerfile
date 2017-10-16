FROM anapsix/alpine-java:latest

ENV target_version solr-7.0.0-SNAPSHOT
ENV source_version solr-4.10.3-cdh5.12.1

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

