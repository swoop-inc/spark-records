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

package com.swoop.scala_util

/** Simple utility for managing bit flags based on an `Int` (32 bits).
  * The implementation uses mutable state.
  *
  * @param value initial value for the bit flags
  */
class BitFlags(protected var value: Int = 0) extends Serializable {

  /** Sets the value to 0.
    *
    * @return the updated object
    */
  def clear(): this.type = {
    value = 0
    this
  }

  /** Adds the 1 bits from the provided mask to the value.
    *
    * @param mask the bit mask to add (using bit OR)
    * @return the updated object
    */
  def add(mask: Int): this.type = {
    value |= mask
    this
  }

  /** Clears 1 the bits from the provided mask from the value.
    *
    * @param mask the bit mask whose 1 bits will be added
    * @return the updated object
    */
  def remove(mask: Int): this.type = {
    value &= (~mask)
    this
  }

  /** Returns `true` if all 1 bits in the provided mask are set to 1 in the value.
    *
    * @param mask the mask to check
    */
  def containsAll(mask: Int): Boolean =
    (value & mask) == mask

  /** Returns `true` if none of the 1 bits in the provided mask are set to 1 in the value.
    *
    * @param mask the mask to check
    */
  def containsNone(mask: Int): Boolean =
    (value & mask) == 0

  /** Returns `true` if some of the 1 bits in the provided mask are set to 1 in the value.
    *
    * @param mask the mask to check
    */
  def containsSome(mask: Int): Boolean =
    (value & mask) != 0

  /** Returns the underlying value. */
  def toInt: Int = value

}
