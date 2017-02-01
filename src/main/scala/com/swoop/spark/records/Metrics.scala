/*
 * Copyright 2017 Simeon Simeonov and Swoop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.swoop.spark.records

import com.swoop.spark.accumulators.ByKeyAdditiveAccumulator

import scala.collection.JavaConversions._


/** Shared trait defining metrics collection services */
trait Metrics {
  /** Returns the collected metrics. */
  def stats: Map[String, Long]

  /** Increments a metric, starting from 0. */
  def inc(stat: String, value: Long = 1): Unit

  /** Prints collected metrics to STDOUT. */
  def printStats(): Unit = {
    val seq = stats.toArray.sorted
    seq.foreach { case (name, value) =>
      println(f"$name%s: $value")
    }
  }
}


/** A [[Metrics]] implementation using [[com.swoop.spark.accumulators.ByKeyAdditiveAccumulator]]. */
trait AccumulatorMetrics extends Metrics {
  def statsAccumulator: ByKeyAdditiveAccumulator[String, Long]

  def stats: Map[String, Long] =
    statsAccumulator.value.toMap

  def inc(stat: String, value: Long = 1): Unit =
    statsAccumulator.add(stat, value)
}
