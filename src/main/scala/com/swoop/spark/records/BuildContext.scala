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

/** Convenience trait that combines the main services exposed by [[RecordBuilder]].
  *
  * @see [[https://swoop-inc.github.io/spark-records/docs.html#advanced-topics-4-build-context-1 Build Context]]
  *
  * @tparam Ctx the job context
  */
trait BuildContext[Ctx] extends HasJobContext[Ctx]
  with HasFeatures with HasDetails with ThrowsErrors
