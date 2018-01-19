FROM anapsix/alpine-java:latest

RUN cd /opt && wget http://archive.apache.org/dist/lucene/solr/5.5.5/solr-5.5.5.tgz && tar xvf solr-5.5.5.tgz
RUN cd /opt && wget http://archive.apache.org/dist/lucene/solr/6.6.2/solr-6.6.2.tgz && tar xvf solr-6.6.2.tgz

ADD ./upgrade-to-5.sh /bin/upgrade-to-5.sh