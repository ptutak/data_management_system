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

curl -X POST -F "file=@/home/ptutak/GitHub/av_decision_support_system/infrastructure/flink/jobs/kafka-to-hdfs/target/scala-2.12/KafkaToHdfs-assembly-0.1.jar" http://192.168.2.121:8082/jars/upload | python -m json.tool

curl -X GET http://192.168.2.121:8082/jars | python -m json.tool

curl -X POST -H "Content-Type: application/json" --data '{"programArgsList": ["--bootstrap.servers", "192.168.2.101:9092,192.168.2.101:9092,192.168.2.103:9092,192.168.2.104:9092", "--group.id", "flink-stream-002", "--topic", "internal_sensor_data", "--path", "hdfs://192.168.2.201:9000/streaming-data/internal_sensor_data_2"]}' http://192.168.2.121:8082/jars/c0a3bff5-f6d7-46e3-bd52-ae9497fb1f9f_KafkaToHdfs-assembly-0.1.jar/run | python -m json.tool

curl -X POST -H "Content-Type: application/json" --data '{"programArgsList": ["--bootstrap.servers", "192.168.2.101:9092,192.168.2.101:9092,192.168.2.103:9092,192.168.2.104:9092", "--topic", "test_topic_from_hdfs", "--path", "hdfs://192.168.2.201:9000/streaming-data/internal_sensor_data_2"]}' http://192.168.2.121:8082/jars/496141a1-4623-43ff-80a3-a25785bd6ea8_HdfsToKafka-assembly-0.1.jar/run | python -m json.tool

curl -X DELETE http://192.168.2.121:8082/jars/a5bf64c8-245b-48cd-b40d-4d0eeb2a98ab_HdfsToKafka-assembly-0.1.jar | python -m json.tool
