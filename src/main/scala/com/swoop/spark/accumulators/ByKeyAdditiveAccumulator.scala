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

package com.swoop.spark.accumulators

import java.util.Collections

import org.apache.spark.util.AccumulatorV2

/** An `AccumulatorV2`-style accumulator for collecting a map of sums.
  *
  * This accumulator is used in [[com.swoop.spark.records.AccumulatorMetrics]] to collect
  * metrics about the execution of record building.
  *
  * Through the use of the [[scala.math.Numeric]] typeclass, the implementation can
  * be used with any numeric type as well as any other value type that is
  * "additive" (the implementation only uses `zero` and `plus`).
  *
  * For example:
  *
  * - You could define `plus` to be the equivalent of `add` for another accumulator, e.g.,
  * `LongAccumulator` and then you can accumulate counts, sums and
  * averages by key.
  *
  * - You could define `plus` on a set to be a union operation
  * and then this accumulator will operate as a `collect_set` by key.
  *
  * - You could define `plus` on an array to be concatenation. Combined with a `maxLength`
  * parameter, this would allow a fast "first N by key" action that completes through a
  * single map stage. The normal way, through a transformation, would require a grouping
  * operation and a shuffle.
  *
  * @tparam A Key type for the map to aggregate into.
  * @tparam B Value type, supported by a [[scala.math.Numeric]] typeclass.
  */
class ByKeyAdditiveAccumulator[A, B: Numeric] extends AccumulatorV2[(A, B), java.util.Map[A, B]] {

  // Java maps are faster than Scala maps
  private val _map: java.util.Map[A, B] =
    new java.util.HashMap

  /** Returns a synchronized `java.util.Map`  with the same key and value type
    * as the accumulator.
    */
  override lazy val value: java.util.Map[A, B] =
    Collections.synchronizedMap(_map) // Delaying full synchronization allows merge() to be faster as it uses unsafeAdd()

  override def isZero: Boolean =
    _map.isEmpty

  override def copyAndReset(): ByKeyAdditiveAccumulator[A, B] =
    new ByKeyAdditiveAccumulator()

  override def copy(): ByKeyAdditiveAccumulator[A, B] = {
    val newAcc = new ByKeyAdditiveAccumulator[A, B]
    _map.synchronized {
      newAcc._map.putAll(_map)
    }
    newAcc
  }

  override def reset(): Unit =
    _map.clear()

  override def add(v: (A, B)): Unit =
    _map.synchronized {
      unsafeAdd(v._1, v._2)
    }

  override def merge(other: AccumulatorV2[(A, B), java.util.Map[A, B]]): Unit =
    other match {
      case o: ByKeyAdditiveAccumulator[A, B] =>
        _map.synchronized {
          other.synchronized {
            import scala.collection.JavaConversions._
            o._map.foreach((unsafeAdd _).tupled)
          }
        }
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

  private def unsafeAdd(k: A, v: B) = {
    val num = implicitly[Numeric[B]]
    val existing = if (_map.containsKey(k)) _map.get(k) else num.zero
    _map.put(k, num.plus(existing, v))
  }

}
