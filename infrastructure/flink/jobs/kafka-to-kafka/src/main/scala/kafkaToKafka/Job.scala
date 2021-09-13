package kafkaToKafka

/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema

object Job {
  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val source_properties = new Properties()
    val sink_properties = new Properties()

    if (
      !(
        parameters.has("source.bootstrap.servers")
        && parameters.has("source.topic")
        && parameters.has("source.group.id")
      )
    ) {
      throw new Exception("Missing source mandatory parameters")
    }
    if (
      !(
        parameters.has("sink.bootstrap.servers")
        && parameters.has("sink.topic")
      )
    ) {
      throw new Exception("Missing sink mandatory parameters")
    }

    source_properties.setProperty("bootstrap.servers", parameters.get("source.bootstrap.servers"))
    source_properties.setProperty("group.id", parameters.get("source.group.id"))
    sink_properties.setProperty("bootstrap.servers", parameters.get("sink.bootstrap.servers"))
    var checkpointIntervalMillis = 5000
    if (parameters.has("checkpoint.interval.millis")) {
      checkpointIntervalMillis = parameters.get("checkpoint.interval.millis").toInt
    }
    env.enableCheckpointing(checkpointIntervalMillis)
    val source: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer[String](
        parameters.get("source.topic"),
        new SimpleStringSchema(),
        source_properties
      )
    )
    val sink = new FlinkKafkaProducer[String](
      parameters.get("sink.topic"),
      new SimpleStringSchema(),
      sink_properties
    )
    source.addSink(sink)
    env.execute(
      "Source: "
        + parameters.get("source.bootstrap.servers")
        + ";"
        + parameters.get("source.topic")
        + "; Sink: "
        + parameters.get("sink.bootstrap.servers")
        + ";"
        + parameters.get("sink.topic")
    )
  }
}
