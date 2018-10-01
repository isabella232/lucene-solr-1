#!/usr/bin/env bash
set -ex
BUILD_OPTS="-Dslf4j.binding=slf4j-log4j12 -Dexclude.from.war=nothing   \
            -Divy.home=${HOME}/.ivy2 -Drepo.maven.org=$IVY_MIRROR_PROP \
            -Divy_install_path=${HOME}/.ant/lib -lib ${HOME}/.ant/lib  \
            -Dreactor.repo=file://${HOME}/.m2/repository \
            -DskipRegexChecksum=.*-cdh6\..* \
            -DuseLocalJavadocUrl=true"

ant $BUILD_OPTS ivy-bootstrap
ant $BUILD_OPTS clean compile compile-test precommit
ant $BUILD_OPTS -buildfile solr/build.xml package
ln -s `pwd`/solr/package/solr-7.4.0-SNAPSHOT.tgz cloudera/solr-upgrade/src/test/resources/solr/dockerfiles/solr_all/solr-7.4.0-SNAPSHOT.tgz
mvn test -f cloudera/solr-upgrade/pom.xml -Psolr-dependencies-available-in-maven,\!skip-tests -B
echo "Pass!"
