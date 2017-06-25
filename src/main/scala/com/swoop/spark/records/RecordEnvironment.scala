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

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, functions => f}

import scala.annotation.implicitNotFound


/** Base class for the implicits that provide hints about how records are persisted. */
@implicitNotFound("Unable to find an implicit record environment for this record type. " +
  "An implicit environment, e.g., FlatRecordEnvironment or PartitionedRecordEnvironment " +
  "must be visible for many methods in Spark Records to work. Please, create one.")
abstract class RecordEnvironment extends Serializable {

  val REQUIRED_RECORD_FIELDS = Seq("features", "data", "source", "flight", "issues")

  /** Returns any custom fields in the record, beyond [[REQUIRED_RECORD_FIELDS]].
    *
    * By default, there are none.
    */
  def customRecordFields: Seq[String] = Seq.empty

  /** Returns a list of fields to add to [[recordData]] beyond `data.*`.
    *
    * By default, there are none.
    */
  def customDataFields: Seq[String] = Seq.empty

  /** Returns the concatenation of [[REQUIRED_RECORD_FIELDS]] and [[customRecordFields]].
    */
  def allRecordFields: Seq[String] = REQUIRED_RECORD_FIELDS ++ customRecordFields

  /** Returns a filter that identifies error records. */
  def errorFilter: Column

  /** Returns a filter that identifies data records.
    * The default implementation is to invert [[errorFilter]].
    */
  def dataFilter: Column = f.not(errorFilter)

  /** Returns only the error records in the provided dataset */
  def errorRecords[Rec](dsRecords: Dataset[Rec])(implicit ev: Rec <:< Record[_, _]): Dataset[Rec] =
    dsRecords.where(errorFilter)

  /** Returns only the error records in the provided dataframe */
  def errorRecords(dfRecords: DataFrame): DataFrame =
    dfRecords.where(errorFilter)

  /** Returns a dataframe with the data of data records (the record envelope is removed).
    * Custom data fields are added.
    */
  def recordData(dfRecords: DataFrame): DataFrame =
    dfRecords.where(dataFilter).select("data.*", customDataFields: _*)

  /** Returns a dataset with the data of data records (the record envelope is removed).
    *
    * @note record data for datasets of `Product` do not include custom data fields.
    *       If you need those you have to go via the `DataFrame` version.
    */
  def recordData[A <: Product, Rec](dsRecords: Dataset[Rec])(implicit ev: Rec <:< Record[A, _], enc: Encoder[A]): Dataset[A] =
    dsRecords.flatMap(_.data)

  // Validate customDataFields
  {
    val unknownFields = customDataFields.diff(customRecordFields)
    if (unknownFields.nonEmpty) {
      throw new Exception(s"customDataFields include the following fields not part of customRecordFields: ${unknownFields.mkString(", ")}")
    }
  }

}


/** A flat record environment.
  *
  * @param customRecordFields Any custom fields that are part of the records.
  * @param customDataFields   Any custom fields from the record to include in record data.
  */
class FlatRecordEnvironment(
  override val customRecordFields: Seq[String] = Seq.empty,
  override val customDataFields: Seq[String] = Seq.empty
) extends RecordEnvironment {

  /** Returns an error filter that selects all records where `features` has the error bit set. */
  def errorFilter: Column = f.col("features").bitwiseAND(Issue.Error.featureMask) =!= 0

}


object FlatRecordEnvironment {
  def apply(customRecordFields: Seq[String] = Seq.empty, customDataFields: Seq[String] = Seq.empty) =
    new FlatRecordEnvironment(customRecordFields, customDataFields)
}


/** A partitioned error environment.
  *
  * @param colName            The record category partition column name.
  *                           Conventionally, this is `par_cat`.
  * @param errorValue         The category value for the partition with error records.
  *                           Conventionally, this is `bad`.
  * @param customRecordFields Any custom fields that are part of the records.
  * @param customDataFields   Any custom fields from the record to include in record data.
  */
class PartitionedRecordEnvironment(
  colName: String,
  errorValue: String,
  override val customRecordFields: Seq[String] = Seq.empty,
  override val customDataFields: Seq[String] = Seq.empty
) extends RecordEnvironment {

  /** Returns an error filter that only includes the partitions with error data */
  def errorFilter: Column = f.col(colName) === errorValue

  /** Returns an error filter that excludes the partitions with error data */
  override def dataFilter: Column = f.col(colName) =!= errorValue

}


object PartitionedRecordEnvironment {
  def apply(colName: String, errorValue: String, customRecordFields: Seq[String] = Seq.empty, customDataFields: Seq[String] = Seq.empty) =
    new PartitionedRecordEnvironment(colName, errorValue, customRecordFields, customDataFields)
}
