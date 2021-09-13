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

from pprint import pformat

import click
import requests


@click.command()
@click.option(
    "--flink-master-host",
    required=True,
    default="http://192.168.2.121:8081",
)
@click.option(
    "--jar-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    required=True,
)
def upload(flink_master_host: str, jar_path: str):
    # for example:
    # jar_path = "kafka-to-hdfs/target/scala-2.12/KafkaToHdfs-assembly-0.1.jar"
    upload_url = f"{flink_master_host}/jars/upload"
    files = {"file": (jar_path, open(jar_path, "rb"), "application/x-java-archive")}
    response = requests.post(upload_url, files=files)
    print(pformat(response.json()))


if __name__ == "__main__":
    upload()
