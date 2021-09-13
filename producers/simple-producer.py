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

import datetime
import traceback
import uuid
import os
from abc import ABC, abstractmethod
from time import sleep
from typing import Any, Dict, Iterator, List, Union

import click
import yaml
from confluent_kafka import Producer


class Record(ABC):
    @property
    @abstractmethod
    def json(self) -> Dict[str, Union[str, int, List, Dict]]:
        """ Returns a json representation of the record """

    @property
    @abstractmethod
    def csv(self) -> str:
        """ Returns csv representation """

    @property
    @abstractmethod
    def key(self) -> bytes:
        """ Returns key representation """


class RecordFactory(ABC):
    def __init__(self, interval: float):
        self.interval = interval

    @abstractmethod
    def get_record(self) -> Record:
        """Get record."""

    def __iter__(self) -> Iterator[Record]:
        while True:
            yield self.get_record()
            sleep(self.interval)



class SelfOrginizingRecord(Record):
    def __init__(
        self, key_name, **kwargs
    ):
        self._key_name = key_name
        self._params = kwargs

    @property
    def json(self) -> Dict[str, Any]:
        return self._params

    @property
    def csv(self) -> str:
        return ",".join(str(value) for value in self._params.values())

    @property
    def key(self) -> bytes:
        return bytes(str(self._params[self._key_name]), encoding="utf8")


class InternalFactory(RecordFactory):
    def get_record(self) -> Record:
        return SelfOrginizingRecord(
            "timestamp",
            timestamp=int(datetime.datetime.now().timestamp() * 10**6),
            my_column=f"{uuid.uuid4()}-my-value",
            name=os.getenv("NAME", "default_name"),
        )


def load_kafka_config(path: str) -> Dict[str, Any]:
    with open(path) as config:
        loaded = yaml.safe_load(config)
        return loaded


def load_avro_schema(schema_file: str):
    with open(schema_file, "rb") as file:
        schema = file.read()
        value_schema = avro.schema.parse(schema)
    return value_schema


@click.command()
@click.option(
    "--kafka-config",
    type=click.Path(exists=True, file_okay=True, readable=True, dir_okay=False),
    required=True,
    help="Kafka config file",
)
@click.option(
    "--topic",
    type=str,
    required=True,
    help="Topic name",
)
@click.option("--time-interval", required=True, type=float, help="Time interval")
def start_producer(
    kafka_config: str,
    topic: str,
    time_interval: float,
):
    config = load_kafka_config(kafka_config)
    producer_config = {"bootstrap.servers": config["bootstrap.servers"]}
    producer = Producer(producer_config)
    factory = InternalFactory(time_interval)
    for record in iter(factory):
        send_records(producer, topic, record)


def send_records(producer: Producer, topic: str, record: Record):
    producer.poll(0.0)
    try:
        key = record.key
        data = record.csv
        producer.produce(
            topic=topic,
            key=key,
            value=data,
            on_delivery=delivery_report,
        )
        print("Request successfull:", topic, key, data)
    except Exception as e:
        print(f"Exception while producing to topic {topic} - {record.json}: {e}")
        print("".join(traceback.TracebackException.from_exception(e).format()))
    producer.flush()


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print(
        "Record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


if __name__ == "__main__":
    start_producer()
