FROM anapsix/alpine-java:latest

ARG docker_source_version

ENV target_version solr-7.0.0-SNAPSHOT
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

