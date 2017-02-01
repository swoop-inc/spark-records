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

import scala.util.Try
import scala.util.control.NonFatal


/** Base class for record builders that produce multiple records for one input.
  *
  * @see [[https://swoop-inc.github.io/spark-records/docs.html##advanced-topics-4-composite-builders-0 Composite Builders]]
  * @see [[NestedRecordBuilder]]
  * @param input      The input data for building the record.
  * @param jobContext the job context
  * @tparam BuildInput  The input type for the builder.
  * @tparam RecordInput The input type records are built from.
  * @tparam Rec         The record type.
  * @tparam Ctx         The job context type
  */
abstract class CompositeRecordBuilder[BuildInput, RecordInput, Rec <: Record[_, _], Ctx <: JobContext]
(input: BuildInput, override val jobContext: Ctx) extends RecordBuilderLike[Rec, Ctx](jobContext) {

  /** Builds the records from the input by partitioning the build input and invoking
    * the provided function, which builds each record.
    */
  protected def buildRecords(fn: RecordInput => TraversableOnce[Rec]): TraversableOnce[Rec]

  /** Returns a record builder for building each record. */
  protected def recordBuilder(singleRecordInput: RecordInput): BuildsRecords[Rec]

  /** Returns the records built from the input.
    *
    * This is the main public API for builder clients. */
  def build: TraversableOnce[Rec] = {
    collectInputStatistics()
    Try(buildRecords(buildPartition))
      .recover { case NonFatal(ex) => Seq(unhandledException(ex)) }
      .get
  }

  protected def buildPartition(recordInput: RecordInput): TraversableOnce[Rec] =
    Try(recordBuilder(recordInput).build)
      .recover { case NonFatal(ex) => Seq(unhandledException(ex)) }
      .get

  override protected def unhandledException(ex: Throwable): Rec = {
    val record = super.unhandledException(ex)
    Try(collectIssueStats(record))
    record
  }

}
