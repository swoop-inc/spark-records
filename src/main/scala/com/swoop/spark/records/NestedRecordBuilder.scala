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


/** Base class for nested record builders of single records.
  *
  * @param input      source data
  * @param jobContext the job context
  * @tparam Input The source input
  * @tparam Data  The record data
  * @tparam Rec   The record
  * @tparam Ctx   The job context
  */
abstract class NestedRecordBuilder[Input, Data <: Product, Rec <: Record[Data, _], Ctx <: JobContext]
(input: Input, override val jobContext: Ctx) extends RecordBuilder[Input, Data, Rec, Ctx](input, jobContext) {

  /** Overrides method and does not increment `input.count`. */
  override def collectInputStatistics(): Unit = {
    // noop for inner builder
  }

}
