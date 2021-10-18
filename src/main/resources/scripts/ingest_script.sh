#!/bin/sh

export SPARK_MAJOR_VERSION=2

spark-submit --driver-memory 4g  --driver-java-options='-Dappconfig=/var/tellme/conf/appconfig.properties  \
-DviewDefLocation=/var/tellme/conf/viewDefinition.json' --class com.tfs.optimus.Optimus /var/tellme/jars/optimus.jar \
$1 $2 $3 $4
