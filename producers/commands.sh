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

python3 /home/ptutak/GitHub/av_decision_support_system/producers/internal-sensor-csv.py --kafka-config /home/ptutak/GitHub/av_decision_support_system/producers/kafka-config.yml --schema-file /home/ptutak/GitHub/av_decision_support_system/diagrams/data_model/av_mobile_data.internal_sensor_data.avsc --topic test_kafka_topic --time-interval 1.0
