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

import org.apache.spark.sql.{Column, DataFrame, SQLContext, functions => f}


/** Base trait for root cause analysis operations. */
trait RootCauseAnalysisOps extends Serializable {

  /** Returns the records. */
  def records: DataFrame

  /** Returns the issues in the records, including all record fields also. */
  def issues: DataFrame

  /** Returns a summary of issue counts by issue category and issue ID. */
  def issueCounts: DataFrame

  /** Returns a summary of issue counts by issue category and issue message. */
  def messageCounts: DataFrame

  /** Returns error details for the first stack element matching the filter,
    * grouped by the provided columns.
    */
  def errorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame

  /** Returns the output of [[errorDetails]], grouped by location. */
  def errorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame
}


/** Provides root cause analysis operations at the record level. */
class RecordsDataFrame(df: DataFrame, env: RecordEnvironment) extends RootCauseAnalysisOps {
  private val sqlc = df.sqlContext

  import sqlc.implicits._

  def records: DataFrame = df

  def issues: DataFrame = df
    .withColumn("record_row_id", f.monotonically_increasing_id())
    .select(f.col("*"), f.posexplode('issues).as(Seq("issue_pos", "issue")))
    .withColumn("category_type", f
      .when($"issue.category" === Issue.Debug.featureMask, Issue.Debug.messageType)
      .when($"issue.category" === Issue.Info.featureMask, Issue.Info.messageType)
      .when($"issue.category" === Issue.Warning.featureMask, Issue.Warning.messageType)
      .when($"issue.category" === Issue.Error.featureMask, Issue.Error.messageType)
      .otherwise(null)
    )
    .withColumn("id_message", f.expr("message_for_issue_id(issue.category, issue.id)"))

  def errorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
    new IssuesDataFrame(issues, env).errorDetails(stackElementFilter)

  def issueCounts: DataFrame =
    new IssuesDataFrame(issues, env).issueCounts

  def messageCounts: DataFrame =
    new IssuesDataFrame(issues, env).messageCounts

  def errorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
    new IssuesDataFrame(issues, env).errorDetailCounts(stackElementFilter)

}


/** Provides root cause analysis operations at the issues level. */
class IssuesDataFrame(df: DataFrame, env: RecordEnvironment) extends RootCauseAnalysisOps {
  private val sqlc = df.sqlContext

  import sqlc.implicits._

  def records: DataFrame = {
    val aggregates = env.allRecordFields.map(name => f.first(f.col(name)).as(name))
    df
      .groupBy('record_row_id)
      .agg(aggregates.head, aggregates.tail: _*)
  }

  def issues: DataFrame = df

  def errorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame = df
    .where($"issue.category" === Issue.Error.categoryId)
    .select(f.col("*"), f.posexplode($"issue.causes").as(Seq("cause_pos", "cause")))
    .select(f.col("*"), f.posexplode($"cause.stack").as(Seq("stack_element_pos", "stack_element")))
    .where(stackElementFilter)
    .groupBy(groupByCols.head, groupByCols.tail: _*)
    .agg(
      f.first($"issue.message").as("message"),
      f.first($"issue.id").as("id"),
      f.first($"id_message").as("id_message"),
      f.first('stack_element).as("location"),
      sampleRecord
    )
    .drop("record_row_id")

  def issueCounts: DataFrame =
    df
      .groupBy($"issue.category", $"issue.id")
      .agg(
        f.first('category_type).as("category_type"),
        f.first('id_message).as("id_message"),
        f.count("*").as("cnt"),
        f.collect_set($"issue.message").as("messages"),
        sampleRecord
      )
      .orderBy('category, 'cnt.desc)
      .drop("category")

  def messageCounts: DataFrame =
    df
      .groupBy($"issue.category", $"issue.message")
      .agg(
        f.first('category_type).as("category_type"),
        f.count("*").as("cnt"),
        f.collect_set($"issue.id").as("ids"),
        f.collect_set('id_message).as("id_messages"),
        f.when(f.sum(f.when('id_message.isNull, 1).otherwise(0)) > 0, true).otherwise(false).as("unknown"),
        sampleRecord
      )
      .drop("category")
      .orderBy('cnt.desc)

  def errorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
    errorDetails(stackElementFilter, groupByCols)
      .groupBy('location)
      .agg(
        f.count("*").as("cnt"),
        f.collect_set('message).as("messages"),
        f.collect_set('id_message).as("id_messages"),
        f.collect_set('id).as("ids"),
        f.first('sample_record).as("sample_record")
      )
      .orderBy('cnt.desc)

  private def sampleRecord = {
    val structExpr = env.allRecordFields.map(name => s"'$name', $name").mkString("named_struct(", ",", ")")
    f.first(f.expr(structExpr)).as("sample_record")
  }

}


/** Provides root cause analysis operations at any level by automatically dispatching
  * to [[RecordsDataFrame]] or [[IssuesDataFrame]] based on the schema of the provided
  * dataframe.
  */
class RootCauseAnalysis(df: DataFrame, env: RecordEnvironment) extends RootCauseAnalysisOps {

  RootCauseAnalysis.registerUDFs(df.sqlContext)

  lazy val proxy: RootCauseAnalysisOps = {
    // @todo tighter schema validation
    val columns = df.columns
    if (columns contains "issue") new IssuesDataFrame(df, env)
    else new RecordsDataFrame(df, env)
  }

  def records: DataFrame = proxy.records

  def issues: DataFrame = proxy.issues

  def issueCounts: DataFrame = proxy.issueCounts

  def messageCounts: DataFrame = proxy.messageCounts

  def errorDetails(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
    proxy.errorDetails(stackElementFilter, groupByCols)

  def errorDetailCounts(stackElementFilter: Column = f.lit(true), groupByCols: Seq[String] = Seq("record_row_id")): DataFrame =
    proxy.errorDetailCounts(stackElementFilter, groupByCols)

}


object RootCauseAnalysis {

  /** Registers the `message_for_issue_id()` UDF, which uses [[Issue$.idMessageFor]]. */
  def registerUDFs(sqlc: SQLContext): Unit = {
    sqlc.udf.register("message_for_issue_id", Issue.idMessageFor _)
  }

  /** Returns a filter that checks whether the `className` of a `StackTraceElement`
    * contains the provided fragment.
    */
  def classNameContains(classNameFragment: String): Column =
    f.instr(f.col("stack_element.className"), classNameFragment) > 0

  /** Filters a dataset unwound to the stack element level by whether the issue is unknown and
    * the provided stack element filter is matching.
    */
  def unknownErrors(stackElementFilter: Column): Column =
    f.col("issue.id").isNull and stackElementFilter

}
