#!/bin/sh
export SPARK_MAJOR_VERSION=2

spark-submit --class com.tfs.dp.spartan.SpartanMaterializationApp --deploy-mode cluster --master yarn --queue default \
--jars hdfs://psr-dap-druid02.app.shared.int.sv2.247-inc.net:8020/user/guruc/spartan_java/lib/spark-avro_2.11-3.2.1-SNAPSHOT.jar,hdfs://psr-dap-druid02.app.shared.int.sv2.247-inc.net:8020/user/guruc/spartan_java/lib/nscala-time_2.11-2.16.0.jar --num-executors 1 --executor-cores 1 --driver-memory 512m --executor-memory 1g hdfs://psr-dap-druid02.app.shared.int.sv2.247-inc.net:8020/user/guruc/spartan_java/lib/spartan_2.11-1.0-SNAPSHOT.jar \
$1 $2 / $3 $4
