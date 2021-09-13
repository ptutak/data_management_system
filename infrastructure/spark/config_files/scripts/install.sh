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

while "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode get | grep "ON"
do
    echo "Waiting for a namenode to leave safe mode."
    sleep 5
done

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$SPARK_HDFS_HOME/event-log"
"$HADOOP_HOME/bin/hdfs" dfs -put "$SPARK_HOME/jars" "$SPARK_YARN_JARS"


if "$HADOOP_HOME/bin/hdfs" dfs -ls "$SPARK_YARN_PHOENIX_JARS"
then
    "$HADOOP_HOME/bin/hdfs" dfs -rm -r "$SPARK_YARN_PHOENIX_JARS"
fi

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$SPARK_YARN_PHOENIX_JARS"
"$HADOOP_HOME/bin/hdfs" dfs -put -f "$PHOENIX_CLIENT_JAR" "$SPARK_YARN_PHOENIX_JARS/"
"$HADOOP_HOME/bin/hdfs" dfs -put -f "$PHOENIX_CONNECTOR_JAR" "$SPARK_YARN_PHOENIX_JARS/"
