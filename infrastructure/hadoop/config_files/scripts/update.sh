#!/bin/bash
# Copyright 2021 Piotr Tutak

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

while "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode get | grep "ON"; do
    echo "Waiting for a namenode to leave safe mode."
    sleep 5
done
"$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /
"$HADOOP_HOME/bin/hdfs" dfs -mkdir /tmp
"$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /tmp
"$HADOOP_HOME/bin/hdfs" dfs -mkdir /user

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/hadoop
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/hadoop/yarn
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/hadoop/hdfs

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/hbase
"$HADOOP_HOME/bin/hdfs" dfs -chown -R hbase /user/hbase

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/flink
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/flink/ha
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/flink/completed-jobs
"$HADOOP_HOME/bin/hdfs" dfs -chown -R flink /user/flink

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/spark
"$HADOOP_HOME/bin/hdfs" dfs -chown -R spark /user/spark

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/support_orchestrator
"$HADOOP_HOME/bin/hdfs" dfs -chown -R support_orchestrator /user/support_orchestrator
