package kafkaToHdfs

/** Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path

object Job {
  def main(args: Array[String]): Unit = {
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val properties = new Properties()

    if (
      !(
        parameters.has("bootstrap.servers")
        && parameters.has("group.id")
        && parameters.has("topic")
        && parameters.has("hdfs")
        && parameters.has("path")
      )
    ) {
      throw new Exception("Missing mandatory parameters")
    }

    properties.setProperty("bootstrap.servers", parameters.get("bootstrap.servers"))
    properties.setProperty("group.id", parameters.get("group.id"))

    val consumer = new FlinkKafkaConsumer[String](
      parameters.get("topic"),
      new SimpleStringSchema(),
      properties
    )

    if (parameters.has("start.from.earliest")) {
      consumer.setStartFromEarliest()
    }

    if (parameters.has("start.from.latest")) {
      consumer.setStartFromLatest()
    }

    val source: DataStream[String] = env.addSource(consumer)
    var maxPartSizeKB = 4
    var rolloverIntervalSec = 15
    var inactivityIntervalSec = 180
    var checkpointIntervalMillis = 5000
    if (parameters.has("max.part.size.kb")) {
      maxPartSizeKB = parameters.get("max.part.size.kb").toInt
    }
    if (parameters.has("rollover.interval.sec")) {
      rolloverIntervalSec = parameters.get("rollover.interval.sec").toInt
    }
    if (parameters.has("inactivity.interval.sec")) {
      inactivityIntervalSec = parameters.get("inactivity.interval.sec").toInt
    }
    if (parameters.has("checkpoint.interval.millis")) {
      checkpointIntervalMillis = parameters.get("checkpoint.interval.millis").toInt
    }
    env.enableCheckpointing(checkpointIntervalMillis)
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path(parameters.get("hdfs") + parameters.get("path")),
        new SimpleStringEncoder[String]("UTF-8")
      )
      .withRollingPolicy(
        DefaultRollingPolicy
          .builder()
          .withRolloverInterval(TimeUnit.SECONDS.toMillis(rolloverIntervalSec))
          .withInactivityInterval(TimeUnit.SECONDS.toMillis(inactivityIntervalSec))
          .withMaxPartSize(maxPartSizeKB * 1024)
          .build()
      )
      .build()

    source.addSink(sink)
    env.execute(
      "Source: "
        + parameters.get("bootstrap.servers")
        + ";"
        + parameters.get("topic")
        + "; Sink: "
        + parameters.get("hdfs")
        + parameters.get("path")
    )
  }
}
