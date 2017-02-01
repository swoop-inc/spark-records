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


/** Shared behaviors for the build context that provides services to builders but does not
  * work directly with Spark context APIs, allowing fast Spark-less testing.
  *
  * @see [[https://swoop-inc.github.io/spark-records/docs.html#advanced-topics-4-build-context-1 Build Context]]
  */
trait JobContext extends Serializable with Metrics with Flight


/** Basic implementation of [[JobContext]] that uses [[AccumulatorMetrics]].
  *
  * @note Spark accumulators can be used locally without registration.
  * @see [[https://swoop-inc.github.io/spark-records/docs.html#advanced-topics-4-build-context-1 Build Context]]
  */
class SimpleJobContext(
  override val flight: Option[String],
  override val statsAccumulator: ByKeyAdditiveAccumulator[String, Long]
) extends JobContext with AccumulatorMetrics {

  /** Raises an exception if the metrics collected during job execution do not satisfy the
    * provided parameters.
    *
    * @see [[SimpleJobContext$.checkDataQuality]]
    */
  def checkDataQuality(minInputs: Long, maxErrorRate: Double, maxSkippedRate: Double): Unit =
    SimpleJobContext.checkDataQuality(this, minInputs, maxErrorRate, maxSkippedRate)

}


object SimpleJobContext {

  def apply(
    flight: Option[String] = None,
    acc: ByKeyAdditiveAccumulator[String, Long] = new ByKeyAdditiveAccumulator
  ) = new SimpleJobContext(flight, acc)

  /** Raises an exception if the metrics collected during job execution do not satisfy the
    * provided parameters.
    *
    * @see [[SimpleJobContext$.checkDataQuality]]
    * @param jc job context
    * @param minInputs minimum number of inputs to receive for the job to succeed
    * @param maxErrorRate max error rate (#error records / #data records) for the job to succeed
    * @param maxSkippedRate max skip rate (#skipped records / #data records) for the job to succeed
    */
  def checkDataQuality(
    jc: JobContext,
    minInputs: Long,
    maxErrorRate: Double,
    maxSkippedRate: Double
  ): Unit = {
    val stats: scala.collection.Map[String, Long] = jc.stats
    val inputs = stats.getOrElse("input.count", 0L)
    val errors = stats.getOrElse("record.error.count", 0L).toDouble
    val skipped = stats.getOrElse("record.skipped.count", 0L).toDouble
    val withData = stats.getOrElse("record.data.count", 0L).toDouble

    def err(msg: String) =
      throw ThrowableMessage(
        Issue.Error,
        s"$msg (inputs: ${inputs.toLong}, skipped: ${skipped.toLong}, errors: ${errors.toLong}, withData: ${withData.toLong})",
        Issue.Error.DATA_QUALITY_CHECK_FAILED
      )

    if (inputs < minInputs) err(s"Suspiciously few inputs.")
    if (errors / withData > maxErrorRate) err(s"Too many errors.")
    if (skipped / withData > maxSkippedRate) err(s"Suspiciously many skipped inputs.")
  }

}
