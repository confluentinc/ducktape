#!/bin/bash -e

# Detect jdk version
jdk=`javac -version 2>&1 | cut -d ' ' -f 2`
ver=`echo $jdk | cut -d '.' -f 2`
if (( $ver > 6 )); then
    echo "Found jdk version $jdk"
    echo "You should only build with jdk 1.6 or below."
    exit 1
fi

GIT_MODE="git@github.com:"
while [ $# -gt 0 ]; do
  OPTION=$1
  case $OPTION in
    --update)
      UPDATE="yes"
      shift
      ;;
    --http)
      GIT_MODE="https://github.com/"
      shift
      ;;
    *)
      break
      ;;
  esac
done

KAFKA_VERSION=0.8.2-beta

if [ ! -e kafka ]; then
    echo "Cloning Kafka"
    git clone http://git-wip-us.apache.org/repos/asf/kafka.git kafka
fi

pushd kafka

if [ "x$UPDATE" == "xyes" ]; then
    echo "Updating Kafka"
    git fetch origin
fi

git checkout tags/$KAFKA_VERSION

# FIXME we should be installing the version of Kafka we built into the local
# Maven repository and making sure we specify the right Kafka version when
# building our own projects. Currently ours link to whatever version of Kafka
# they default to, which should work ok for now.
echo "Building Kafka"
KAFKA_BUILD_OPTS=""
if [ "x$SCALA_VERSION" != "x" ]; then
    KAFKA_BUILD_OPTS="$KAFKA_BUILD_OPTS -PscalaVersion=$SCALA_VERSION"
fi
if [ ! -e gradle/wrapper/ ]; then
    gradle
fi
./gradlew $KAFKA_BUILD_OPTS jar
popd

function build_maven_project() {
    NAME=$1
    URL=$2
    # The build target can be specified so that shared libs get installed and
    # can be used in the build process of applications, but applications only
    # need to build enough to be tested.
    BUILD_TARGET=$3

    if [ ! -e $NAME ]; then
        echo "Cloning $NAME"
        git clone $URL $NAME
    fi

    # Turn off tests for the build because some of these are local integration
    # tests that take a long time. This shouldn't be a problem since these
    # should be getting run elsewhere.
    BUILD_OPTS="-DskipTests"
    if [ "x$SCALA_VERSION" != "x" ]; then
        BUILD_OPTS="$BUILD_OPTS -Dkafka.scala.version=$SCALA_VERSION"
    fi

    pushd $NAME

    if [ "x$UPDATE" == "xyes" ]; then
        echo "Updating $NAME"
        git pull origin
    fi

    echo "Building $NAME"
    mvn $BUILD_OPTS $BUILD_TARGET
    popd
}

build_maven_project "common" "${GIT_MODE}confluentinc/common.git" "install"
build_maven_project "rest-utils" "${GIT_MODE}confluentinc/rest-utils.git" "install"
build_maven_project "schema-registry" "${GIT_MODE}confluentinc/schema-registry.git" "install"
build_maven_project "kafka-rest" "${GIT_MODE}confluentinc/kafka-rest.git" "package"
