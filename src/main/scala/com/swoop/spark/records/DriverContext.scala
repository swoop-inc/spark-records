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
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/** Shared behaviors for the build context that integrates with Spark context.
  *
  * @see [[https://swoop-inc.github.io/spark-records/docs.html#advanced-topics-4-build-context-1 Build Context]]
  */
trait DriverContext extends Serializable with Metrics with Flight


/** Basic implementation of [[DriverContext]] that uses [[AccumulatorMetrics]] and
  * includes a convenience factory for creating job contexts.
  *
  * @see [[https://swoop-inc.github.io/spark-records/docs.html#advanced-topics-4-build-context-1 Build Context]]
  *
  * @param sc `SparkContext`
  */
class SimpleDriverContext(sc: SparkContext) extends DriverContext with AccumulatorMetrics {

  /** Returns a driver context built from `SparkSession` */
  def this(spark: SparkSession) = this(spark.sparkContext)

  val statsAccumulator = new ByKeyAdditiveAccumulator[String, Long]
  sc.register(statsAccumulator)

  /** Returns a job context initialized by this driver context. */
  def jobContext[Ctx <: JobContext](
    factory: SimpleDriverContext.JobContextFactory[Ctx]): Ctx =
    factory(flight, statsAccumulator)

}


object SimpleDriverContext {

  /** Structured type matching companion objects for job contexts that can be created by
    * [[SimpleDriverContext]].
    *
    * @tparam Ctx the job context
    */
  type JobContextFactory[Ctx <: JobContext] = {
    def apply(
      flight: Option[String],
      statsAccumulator: ByKeyAdditiveAccumulator[String, Long]
    ): Ctx
  }

  def apply(sc: SparkContext) = new SimpleDriverContext(sc)

  def apply(spark: SparkSession) = new SimpleDriverContext(spark)

}
