/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 // modify from https://github.com/yennanliu/flink-streaming-demo/blob/master/src/main/scala/com/dataartisans/flink_demo/examples/TotalArrivalCount.scala
 

package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import org.apache.flink.streaming.api.scala._

/**
 * Apache Flink DataStream API demo application.
 *
 * The program processes a stream of taxi ride events from the New York City Taxi and Limousine
 * Commission (TLC).
 * It computes for each location the total number of persons that arrived by taxi.
 *
 * See
 *   http://github.com/dataartisans/flink-streaming-demo
 * for more detail.
 *
 */
object RunFlinkTest {

  def main(args: Array[String]) {

    // input parameters
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // Elasticsearch parameters
    val writeToElasticsearch = false // set to true to write results to Elasticsearch
    val elasticsearchHost = "" // look-up hostname in Elasticsearch log output
    val elasticsearchPort = 9300

  }

}