#!/bin/bash
set -ex
export JAVA7_BUILD=true
source /opt/toolchain/toolchain.sh
 BUILD_OPTS="-Dversion=${FULL_VERSION}                                  \
            -Dslf4j.binding=slf4j-log4j12 -Dexclude.from.war=nothing   \
            -Divy.home=${HOME}/.ivy2 -Drepo.maven.org=$IVY_MIRROR_PROP \
            -Divy_install_path=${HOME}/.ant/lib -lib ${HOME}/.ant/lib  \
            -Dreactor.repo=file://${HOME}/.m2/repository"

ant $BUILD_OPTS ivy-bootstrap
ant $BUILD_OPTS clean compile compile-test
echo "Pass! "
