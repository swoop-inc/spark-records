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


/** A Spark Record is an envelope around data.
  *
  * @see [[https://swoop-inc.github.io/spark-records/ Spark Records documentation]]
  *
  * @tparam A The data stored in the record
  * @tparam Source The input type
  */
trait Record[A <: Product, Source] extends Product with Serializable {
  type Data = A

  def features: Int
  def data: Option[A]
  def source: Option[Source]
  def flight: Option[String]
  def issues: Option[Seq[Issue]]

  def isError: Boolean = Record.isError(features)
  def isFailure: Boolean = isError
  def isSuccess: Boolean = !isFailure
  def isDefined: Boolean = isSuccess && data.nonEmpty
}


object Record {
  /** Returns `true` if provided parameter contains the error flag */
  def isError(features: Int): Boolean = (features & Features.ERROR) != 0
}
