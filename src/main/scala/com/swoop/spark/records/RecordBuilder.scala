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

import com.swoop.scala_util.BitFlags

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal


/** Topmost trait for record builders. */
trait BuildsRecords[Rec <: Record[_, _]] extends Serializable {

  /** Returns zero or more records based on the builder type and the input. */
  def build: TraversableOnce[Rec]

}


/** Trait for a builder with a job context. */
trait HasJobContext[Ctx] {
  /** Returns the job context. */
  def jobContext: Ctx

  /** Shorthand for [[jobContext]]. */
  def jc: Ctx = jobContext
}


/** Trait for a builder that aggregates features. */
trait HasFeatures {

  /** Returns the feature mask collected during record building. */
  def features: Int

  /** Returns `true` if the feature mask collected during record building contains
    * one of more of the 1 bits in the provided bitmask.
    */
  def containsSomeFeatures(mask: Int): Boolean

  /** Notifies the builder to add the following features to the aggregate features. */
  def notifyFeatures(flags: Int): Unit

}


/** Trait for a builder that aggregates details, which will become issues.
  *
  * Under the covers, details are [[Throwable]] instances: either exceptions thrown by
  * the application code or [[ThrowableMessage]] instances that may be thrown and caught
  * or simply constructed using one of the convenience methods exposed by this trait.
  *
  */
trait HasDetails {

  /** Notifies the builder to add this [[Throwable]] to the list from which issues
    * will be generated.
    *
    * If the provided parameter is extends [[FeatureMask]], its features will be added to
    * the set aggregated by the builder.
    */
  def notifyDetail(ex: Throwable): Unit

  def debug(msg: String): Unit =
    debug(msg, Issue.Debug.UNKNOWN)

  def debug(msg: String, id: Int): Unit =
    notifyDetail(ThrowableMessage(Issue.Warning, msg, id))

  def debug(msg: String, detail: String, id: Int = Issue.Debug.UNKNOWN): Unit =
    warn(Issue.messageWithDetails(msg, detail), id)

  def info(msg: String): Unit =
    debug(msg, Issue.Info.UNKNOWN)

  def info(msg: String, id: Int): Unit =
    notifyDetail(ThrowableMessage(Issue.Info, msg, id))

  def info(msg: String, detail: String, id: Int = Issue.Info.UNKNOWN): Unit =
    warn(Issue.messageWithDetails(msg, detail), id)

  def warn(msg: String): Unit =
    debug(msg, Issue.Warning.UNKNOWN)

  def warn(msg: String, id: Int): Unit =
    notifyDetail(ThrowableMessage(Issue.Warning, msg, id))

  def warn(msg: String, detail: String, id: Int = Issue.Warning.UNKNOWN): Unit =
    warn(Issue.messageWithDetails(msg, detail), id)

  def error(msg: String): Unit =
    debug(msg, Issue.Error.UNKNOWN)

  def error(msg: String, id: Int): Unit =
    notifyDetail(ThrowableMessage(Issue.Error, msg, id))

  def error(msg: String, detail: String, id: Int = Issue.Error.UNKNOWN): Unit =
    error(Issue.messageWithDetails(msg, detail), id)

}


/** Base class for single and composite record builders.
  *
  * @param jobContext the job context
  * @tparam Rec The record
  * @tparam Ctx The job context
  */
abstract class RecordBuilderLike[Rec <: Record[_, _], Ctx <: JobContext](jobContext: Ctx)
  extends BuildsRecords[Rec] with HasJobContext[Ctx] with ThrowsErrors {

  /** Builds an error record. Should never throw an exception. */
  protected def errorRecord(issues: Seq[Issue]): Rec

  /** Returns an error record for an unhandled exception.
    * Collects unhandled error metrics.
    */
  protected def unhandledException(ex: Throwable): Rec = {
    val record = errorRecord(Issue.from(ex))
    Try(collectUnhandledExceptionStatistics(record, ex))
    record
  }

  /** Called once for each top-level `build()` call for `AnyRecordBuilder`.
    */
  protected def collectInputStatistics(): Unit =
    jc.inc("input.count")

  /** Called when there is an internal error as part of record building.
    *
    * An error issue will be created for this exception and [[collectIssueStats]]
    * will take it into account. This method is called in order to specifically
    * capture statistics about this internal error.
    *
    * @param record The record with the unhandled exception in `issues`.
    * @param ex     The unhandled exception.
    */
  protected def collectUnhandledExceptionStatistics(record: Rec, ex: Throwable): Unit =
    jc.inc("record.exception.unhandled.count")

  /** Called every time a record is emitted, independent of whether it has
    * data or not and whether it is an error or not.
    *
    * Will not be called on skipped records.
    *
    * @param record The record about to be emitted.
    */
  protected def collectIssueStats(record: Rec): Unit = {
    jc.inc(s"issue.count", record.issues.size)
    record.issues.foreach { issues =>
      issues.foreach { issue =>
        jc.inc(s"issue.category.${issue.category}")
        issue.id.foreach(id => jc.inc(s"issue.id.${issue.category}-$id"))
      }
    }
  }

}


/** Base class for record builders of single records.
  *
  * @param input      source data
  * @param jobContext the job context
  * @tparam Input The source input
  * @tparam Data  The record data
  * @tparam Rec   The record
  * @tparam Ctx   The job context
  */
abstract class RecordBuilder[Input, Data <: Product, Rec <: Record[Data, _], Ctx <: JobContext]
(val input: Input, override val jobContext: Ctx)
  extends RecordBuilderLike[Rec, Ctx](jobContext)
  with BuildContext[Ctx] {
  self =>

  private var _maybeData: Option[Data] = None
  private val _features = new BitFlags(0)
  private val _details = ArrayBuffer.empty[Throwable]

  /** Returns the data for the record or `None` if no record should be generated for the data.
    */
  protected def buildData: Option[Data]

  /** Returns a data record for the data. */
  protected def dataRecord(data: Data, issues: Seq[Issue]): Rec

  /** Returns the record built from the input or an empty traversable if the input should be skipped. */
  def build: TraversableOnce[Rec] = {
    collectInputStatistics()
    Try(captureData)
      .recover { case ex if NonFatal(ex) =>
        // If an error was generated during building data, save it; this will be an error record
        notifyDetail(ex)
        None
      }
      .map(toRecord)
      .map { maybeRecord => collectStatistics(maybeRecord); maybeRecord }
      .recover { case NonFatal(ex) => Some(unhandledException(ex)) }
      .get
      .map { rec => Try(collectIssueStats(rec)); rec }
  }

  /** Returns the result of `buildData()` after saving it in `_maybeData` for possible use
    * in `errorRecord()`.
    */
  protected def captureData: Option[Data] =
    buildData.map { data =>
      _maybeData = Some(data)
      data
    }

  /** Returns `_maybeData`. */
  protected def maybeData: Option[Data] =
    _maybeData

  /** Returns a record from the data or `None`, if no record should be emitted.
    * For a record to be emitted, there must either be data or an error and
    * The [[Features.SKIP]] flag must not be set in `features`.
    */
  protected def toRecord(maybeData: Option[Data]): Option[Rec] =
  if (containsSomeFeatures(Features.ERROR)) {
      Some(errorRecord(issues))
  } else if (maybeData.isEmpty || containsSomeFeatures(Features.SKIP)) {
      collectSkippedRecordStatistics(maybeData)
      None
    } else {
      Some(dataRecord(maybeData.get, issues))
    }

  // Statistics collection

  /** Orchestrates statistics collection. */
  protected def collectStatistics(maybeRecord: Option[Rec]): Unit =
    maybeRecord match {
      case Some(record) =>
        collectAnyRecordStatistics(record)
        if (record.data.isDefined) {
          assert(!containsSomeFeatures(Features.SKIP))
          collectDataRecordStatistics(record)
        }
        else {
          assert(containsSomeFeatures(Features.ERROR))
          collectErrorRecordStatistics(record)
        }
      case None =>
        collectEmptyRecordStatistics(features)
    }

  def features: Int = _features.toInt

  /** Returns `true` if the feature mask collected during record building contains
    * one of more of the 1 bits in the provided bitmask.
    */
  def containsSomeFeatures(mask: Int): Boolean = _features.containsSome(mask)

  /** Notifies the builder to add the following features to the aggregate features. */
  def notifyFeatures(mask: Int): Unit = _features.add(mask)

  /** Notifies the builder to add this [[Throwable]] to the list from which issues
    * will be generated.
    *
    * If the provided parameter is extends [[FeatureMask]], its features will be added to
    * the set aggregated by the builder.
    */
  def notifyDetail(ex: Throwable): Unit = {
    ex match {
      case FeatureMask(featureMask) => _features.add(featureMask)
      case _ => _features.add(Features.ERROR)
    }
    _details += ex
  }

  // Statistics

  /** Called for every record that is emitted.
    *
    * @param record The record to be emitted.
    */
  protected def collectAnyRecordStatistics(record: Rec): Unit = {
    jc.inc("record.count")
    jc.inc(s"record.features.$features")
  }

  /** Called when record builder produced no data from the record input.
    * Note that this is different from the case of data being produced and the
    * builder deciding that the record should be skipped.
    *
    * @param features The feature mask accumulated by the builder to this point.
    */
  protected def collectEmptyRecordStatistics(features: Int): Unit = {
    jc.inc("record.empty.count")
  }

  /** Called when data was produced that should be skipped.
    *
    * @param maybeData The data to be skipped or `None` if no data was produced.
    */
  protected def collectSkippedRecordStatistics(maybeData: Option[Data]): Unit = {
    jc.inc("record.skipped.count")
  }

  /** Called when a record with non-empty data that should not be skipped
    * is generated.
    *
    * @param record The record with data.
    */
  protected def collectDataRecordStatistics(record: Rec): Unit = {
    jc.inc("record.data.count")
  }

  /** Called when an error record was produced.
    * The record will not have data.
    *
    * @param record The error record.
    */
  protected def collectErrorRecordStatistics(record: Rec): Unit = {
    jc.inc("record.error.count")
  }

  /** Lightens up issues by removing unnecessary stack traces. */
  private def issues: Seq[Issue] =
    _details.flatMap { ex =>
      Issue.from(ex).map {
        case issue@Issue(Issue.Error.categoryId, _, _, _, _) =>
          issue.withShortStackTraces
        case issue =>
          issue.withoutCauses
      }
    }

}
