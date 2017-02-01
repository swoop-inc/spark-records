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

package com.swoop.spark

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row, functions => f}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** Spark Records package.
  *
  * @see [[https://swoop-inc.github.io/spark-records/ Spark Records documentation]]
  */
package object records {

  /** Implicit operations on Spark Records datasets.
    *
    * @see [[RootCauseAnalysisOps]]
    */
  implicit class RecordsRichDataset[Rec <: Record[_, _] : Encoder](val underlying: Dataset[Rec]) {

    def errorRecords(implicit env: RecordEnvironment): Dataset[Rec] =
      env.errorRecords(underlying)

    def allIssues: DataFrame =
      new RootCauseAnalysis(underlying.filter(_.issues.isDefined).toDF()).issues

    def issueCounts: DataFrame =
      issueCounts(false)

    def issueCounts(showSampleRecord: Boolean): DataFrame =
      allIssues.issueCounts(showSampleRecord)

    def messageCounts: DataFrame =
      messageCounts(false)

    def messageCounts(showSampleRecord: Boolean): DataFrame =
      allIssues.messageCounts(showSampleRecord)

    def errorIssues(implicit env: RecordEnvironment): DataFrame =
      new RootCauseAnalysis(errorRecords.toDF()).issues

    def errorIssueCounts(implicit env: RecordEnvironment): DataFrame =
      errorIssues.issueCounts

    def errorMessageCounts(implicit env: RecordEnvironment): DataFrame =
      errorIssues.messageCounts

    def errorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id"))
      (implicit env: RecordEnvironment): DataFrame =
      new RootCauseAnalysis(errorIssues).errorDetails(stackElementFilter, groupByCols)

    def errorDetails(classNameFragment: String)
      (implicit env: RecordEnvironment): DataFrame =
      errorDetails(RootCauseAnalysis.classNameContains(classNameFragment))

    def errorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id"))
      (implicit env: RecordEnvironment): DataFrame =
      new RootCauseAnalysis(errorIssues).errorDetailCounts(stackElementFilter, groupByCols)

    def errorDetailCounts(classNameFragment: String)
      (implicit env: RecordEnvironment): DataFrame =
      errorDetailCounts(RootCauseAnalysis.classNameContains(classNameFragment))

    def unknownErrorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id"))
      (implicit env: RecordEnvironment): DataFrame =
      errorDetails(f.col("issue.id").isNull and stackElementFilter, groupByCols).drop("id_message")

    def unknownErrorDetails(classNameFragment: String)
      (implicit env: RecordEnvironment): DataFrame =
      unknownErrorDetails(RootCauseAnalysis.classNameContains(classNameFragment))

    def unknownErrorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id"))
      (implicit env: RecordEnvironment): DataFrame =
      errorDetailCounts(f.col("issue.id").isNull and stackElementFilter, groupByCols)

    def unknownErrorDetailCounts(classNameFragment: String)
      (implicit env: RecordEnvironment): DataFrame =
      unknownErrorDetailCounts(RootCauseAnalysis.classNameContains(classNameFragment)).drop("id_messages")

    def recordData(implicit env: RecordEnvironment): Dataset[Row] =
      underlying.where(env.dataFilter).select("data.*")

  }


  /** Implicit operations on Spark Records dataframes.
    *
    * Most operations can be applied equally at the record level or at the level of unwound issues.
    *
    * @see [[RootCauseAnalysisOps]]
    * @see [[RootCauseAnalysis]]
    */
  implicit class RCARichDataFrame(val underlying: DataFrame) {

    def records: DataFrame =
      new RootCauseAnalysis(underlying).records

    def issues: DataFrame =
      new RootCauseAnalysis(underlying).issues

    def issueCounts: DataFrame =
      issueCounts(false)

    def issueCounts(showSampleRecord: Boolean): DataFrame =
      new RootCauseAnalysis(underlying).issueCounts
        .drop(if (showSampleRecord) "" else "sample_record")

    def messageCounts: DataFrame =
      messageCounts(false)

    def messageCounts(showSampleRecord: Boolean): DataFrame =
      new RootCauseAnalysis(underlying).messageCounts
        .drop(if (showSampleRecord) "" else "sample_record")

    def errorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
      new RootCauseAnalysis(underlying).errorDetails(stackElementFilter, groupByCols)

    def errorDetails(classNameFragment: String): DataFrame =
      errorDetails(RootCauseAnalysis.classNameContains(classNameFragment))

    def errorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
      new RootCauseAnalysis(underlying).errorDetailCounts(stackElementFilter, groupByCols)

    def errorDetailCounts(classNameFragment: String): DataFrame =
      errorDetailCounts(RootCauseAnalysis.classNameContains(classNameFragment))

    def unknownErrorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
      errorDetails(RootCauseAnalysis.unknownErrors(stackElementFilter), groupByCols)

    def unknownErrorDetails(classNameFragment: String): DataFrame =
      unknownErrorDetails(RootCauseAnalysis.classNameContains(classNameFragment)).drop("id_message")

    def unknownErrorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
      errorDetailCounts(RootCauseAnalysis.unknownErrors(stackElementFilter), groupByCols)

    def unknownErrorDetailCounts(classNameFragment: String): DataFrame =
      unknownErrorDetailCounts(RootCauseAnalysis.classNameContains(classNameFragment)).drop("id_messages")

  }


  /** Convenience conversion to make issue saving during record creation easier. */
  implicit def optionalIssueSeq(issues: Seq[Issue]): Option[Seq[Issue]] =
    if (issues.isEmpty) None else Some(issues)

  /** Extends [[scala.util.Try]] with record building sugar. */
  implicit class RecordsRichTry[A](val underlying: Try[A]) extends AnyVal {

    /** Fails with a [[ThrowableMessage]]. */
    @inline def failWith(msg: => String, id: => Int = Issue.Error.UNKNOWN): Try[A] =
      underlying.recoverWith(new FailWith[A](msg, id))

  }

  /** Extends [[scala.util.Try]] with optional values with record building sugar. */
  implicit class RecordsRichTryOption[A](val underlying: Try[Option[A]]) extends AnyVal {

    /** Fails with a [[ThrowableMessage]] unless there is success and the option is defined. */
    @inline def failUnlessDefined(msg: => String, id: => Int = Issue.Error.UNKNOWN): Try[Option[A]] =
      underlying match {
        case Success(Some(x)) => underlying
        case _ => underlying.recoverWith(new FailWith[Option[A]](msg, id))
      }

  }

  /** Extends [[Option]] with record building sugar. */
  implicit class RecordsRichOption[A](val underlying: Option[A]) extends AnyVal {

    /** Returns the value or fails with a [[ThrowableMessage]] unless the option is defined. */
    @inline def getOrFail(msg: => String, id: => Int = Issue.Error.UNKNOWN): A =
      underlying
        .getOrElse(Issue.throwError(msg, id))

    /** Fails with a [[ThrowableMessage]] unless the option is defined. */
    @inline def orFail(msg: => String, id: => Int = Issue.Error.UNKNOWN): Option[A] =
      underlying
        .orElse(Issue.throwError(msg, id))

  }

  private class FailWith[A](msg: => String, id: => Int) extends PartialFunction[Throwable, Try[A]] {

    def isDefinedAt(ex: Throwable) = NonFatal(ex)

    def apply(ex: Throwable): Try[A] =
      Failure(ThrowableMessage(Issue.Error, msg, id, ex))

  }

}
