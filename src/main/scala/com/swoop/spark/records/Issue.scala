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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** Trait for types that have a feature mask. */
trait FeatureMask {
  def featureMask: Int

  /** Returns `true` it the bit AND between the feature mask and the provided parameter
    * is non-zero.
    */
  def hasFeature(mask: Int): Boolean =
    (featureMask & mask) != 0
}


object FeatureMask {
  def unapply(withFeatureMask: FeatureMask) =
    Some(withFeatureMask.featureMask)
}


/** Convenience trait for throwing [[ThrowableMessage]]. */
trait ThrowsErrors {

  def throwError(message: String, detail: String): Nothing =
    throwError(Issue.messageWithDetails(message, detail))

  def throwError(message: String): Nothing =
    throw ThrowableMessage(Issue.Error, message, Issue.Error.UNKNOWN, null)

  def throwError(message: String, detail: String, messageId: Int): Nothing =
    throwError(Issue.messageWithDetails(message, detail), messageId)

  def throwError(message: String, messageId: Int): Nothing =
    throw ThrowableMessage(Issue.Error, message, messageId, null)

  def throwError(message: String, detail: String, messageId: Int, cause: Throwable): Nothing =
    throwError(Issue.messageWithDetails(message, detail), messageId, cause)

  def throwError(message: String, messageId: Int, cause: Throwable): Nothing =
    throw ThrowableMessage(Issue.Error, message, messageId, cause)

}


/** Base class for throwable diagnostic messages with IDs for quick big data processing.
  *
  * @param kind  the type of message (error, warning, info or debug)
  * @param msg   the message
  * @param msgId the optional message ID
  * @param cause the cause, if the message is being thrown as a result of an exception
  */
case class ThrowableMessage(
  kind: Issue.ThrowableMessageType,
  msg: String,
  msgId: Int = 0,
  cause: Throwable = null
) extends Throwable(msg, cause) with FeatureMask {

  def this(messageType: Issue.ThrowableMessageType, message: String, cause: Throwable) =
    this(messageType, message, 0, cause)

  /** Returns `None` if `msgId` is 0, `Some(msgId)` otherwise. */
  def messageIdOption: Option[Int] =
    if (msgId == 0) None else Some(msgId)

  def featureMask: Int = kind.featureMask

  override def toString = s"$messageKey: $msg"

  def messageKey = f"$messageType$msgId%05d"

  def messageType: String = kind.messageType

  def idMessage: String =
    Issue.idMessageFor(kind.featureMask, msgId).get

}


/** A simple, serializable adapter for [[StackTraceElement]].
  */
case class StackElement(
  className: String,
  methodName: String,
  fileName: String,
  lineNumber: Int
) {
  override def toString: String =
    new StackTraceElement(className, methodName, fileName, lineNumber).toString
}


/** A simple, serializable exception cause that keeps the exception message and the stack trace.
  */
case class Cause(
  message: String,
  stack: Seq[StackElement] = Seq.empty
) {

  def this(ex: Throwable) = this(
    ex.getMessage,
    ex.getStackTrace.map(ste => StackElement(
      ste.getClassName,
      ste.getMethodName,
      ste.getFileName,
      ste.getLineNumber
    ))
  )

  override def toString: String =
    if (stack.isEmpty) s"Cause: $message"
    else s"Cause: $message\n${stack.mkString("\n")}"

}


/** An issue is a container for diagnostic information including both simple fields
  * and an optional sequence of causes.
  *
  * @see [[Issue$.messageWithDetails]]
  * @param category The category of the issue.
  *                 It matches the with the values coming from the different types of
  *                 [[Issue$.ThrowableMessageType]] `featureMask` values.
  * @param message  The message related to the issue.
  * @param causes   The causes of this issue.
  * @param id       The issue ID.
  * @param details  Optional additional details for the issue.
  *                 For root cause analysis purposes it is useful to to split messages we
  *                 want to associate with records into a stable portion (the issue message) and
  *                 a variable portion (the issue details).
  *                 This enables stable grouping and counting.
  */
case class Issue(
  category: Int,
  message: String,
  causes: Option[Seq[Cause]] = None,
  id: Option[Int] = None,
  details: Option[String] = None
) {

  def messageType: String =
    Issue.messageType(category)

  def idMessage: Option[String] =
    id.flatMap(value => Issue.idMessageFor(category, value))

  /** Returns a "ligher" issue without causes */
  def withoutCauses: Issue =
    copy(
      category = category,
      id = id,
      message = message,
      details = details
    )

  /** Returns a "lighter" issue without stack traces in the causes */
  def withoutStackTraces: Issue =
    copy(
      category = category,
      id = id,
      message = message,
      details = details,
      causes = causes.map(causes =>
        causes.map(cause =>
          Cause(cause.message)))
    )

  /** Returns a "lighter" issue by shortening stack traces.
    *
    * Shortening involves eliminating the common postfix, which is usually startup/initialization code.
    */
  def withShortStackTraces: Issue = {
    val myReverseStack = new Cause(new Exception("")).stack.reverse
    copy(
      category = category,
      id = id,
      message = message,
      details = details,
      causes = causes.map(causes =>
        causes.map(cause =>
          Cause(
            cause.message,
            shortenStackTrace(cause.stack, myReverseStack))))
    )
  }

  override def toString =
    s"Issue $category.${id.getOrElse(0)}: $message\nDetails: ${details.getOrElse("N/A")}\nCauses:\n${causes.getOrElse(Seq()).mkString("\n")}"

  private def shortenStackTrace(stack: Seq[StackElement], myReverseStack: Seq[StackElement]): Seq[StackElement] = {
    val commonLength = (stack.view.reverse, myReverseStack).zipped.takeWhile(Function.tupled(_ == _)).size
    stack.slice(0, stack.length - commonLength + 2)
  }

}


object Issue extends ThrowsErrors {

  private final val MESSAGE_DETAILS_SEPARATOR = ":::"
  private final val MessageAndDetails = s"^(.+)\\s+$MESSAGE_DETAILS_SEPARATOR\\s+(.*)$$".r

  private final val categories = Seq(Error, Warning, Info, Debug)
  private final val categoryIds = categories.map(_.categoryId)
  private final val issueTypeMask = categoryIds.reduce(_ | _)

  /** Returns no issues if the result is a [[scala.util.Success]] and
    * one or more issues if the result is a [[scala.util.Failure]].
    */
  def from[A](result: Try[A]): Seq[Issue] = result match {
    case Success(_) => Seq.empty
    case Failure(NonFatal(ex)) => from(ex)
  }

  /** Returns one or more issues created from the parameter.
    *
    * Information from [[ThrowableMessage]] subclasses is extracted in full fidelity.
    */
  def from(ex: Throwable): Seq[Issue] = {
    val exMsg = ex.toString

    val (message, maybeDetails) = exMsg match {
      case MessageAndDetails(msg, details) => (msg, Some(details))
      case _ => (exMsg, None)
    }

    val (maybeId, category) = ex match {
      case tm: ThrowableMessage => (tm.messageIdOption, tm.featureMask & issueTypeMask)
      case _ => (None, Error.categoryId)
    }

    val maybeCauses: Option[Seq[Cause]] =
      if (category == Error.categoryId) {
        val result = new ArrayBuffer[Cause]
        var current = ex
        do {
          result += new Cause(current)
          current = current.getCause
        } while (current != null)
        Some(result)
      }
      else None

    Seq(Issue(category, message, maybeCauses, maybeId, maybeDetails))
  }

  /** Combines the message and details together using [[MESSAGE_DETAILS_SEPARATOR]].
    * This allows them to be to split apart into `message` and `details` when an [[Issue]]
    * is created.
    */
  def messageWithDetails(message: String, detail: String): String =
    s"$message ${Issue.MESSAGE_DETAILS_SEPARATOR} $detail"

  /** Base trait for issue types. */
  sealed trait ThrowableMessageType extends Serializable with FeatureMask {
    /** Issue category ID, which matches the values from [[Features]] for
      * `ERROR`, `WARNING`, `INFO` and `DEBUG`.
      */
    def categoryId: Int

    /** One character abbreviation: one of `E`, `W`, `I` or `D`. */
    def messageType: String

    /** Descriptions for issue IDs for this type of [[Issue]]. */
    def idMessages: Map[Int, String]
  }

  def messageType(category: Int): String =
    category match {
      case Debug.categoryId => Debug.messageType
      case Info.categoryId => Info.messageType
      case Warning.categoryId => Warning.messageType
      case Error.categoryId => Error.messageType
      case _ => throw new InternalError(s"Unknown category $category")
    }

  private lazy val allIdMessages = {
    val m: Map[Int, ConcurrentHashMap[Int, String]] = Map(
      categoryIds.map((_, new ConcurrentHashMap[Int, String])): _*
    )
    categories.foreach(cat => addMessagesFor(cat.categoryId, cat.idMessages, m))
    m
  }

  private def addMessagesFor(category: Int, idMessages: Map[Int, String], to: Map[Int, ConcurrentHashMap[Int, String]]): Unit = {
    val categoryMap = to(category)
    idMessages.foreach { case (k, v) =>
        categoryMap.put(k, v)
    }
  }

  /** Registers application-specific descriptions for issue IDs.
    *
    * For simplicity, the implementation uses global, thread-safe mutable state.
    * This makes it important to follow the commonsense rule of using unique issue IDs
    * across applications that may be running on the same Spark cluster.
    */
  def addMessagesFor(category: ThrowableMessageType, idMessages: Map[Int, String]): Unit =
    addMessagesFor(category.featureMask, idMessages, allIdMessages)

  /** Returns the message associated with an issue ID with a particular category feature mask. */
  def idMessageFor(category: Int, messageId: Int): Option[String] =
    allIdMessages.get(category).flatMap { msgs =>
      if (msgs.containsKey(messageId)) Some(msgs.get(messageId))
      else None
    }

  object Debug extends ThrowableMessageType {
    val featureMask: Int = Features.DEBUG
    val categoryId: Int = featureMask
    val messageType: String = "D"

    // ID values
    val UNKNOWN = 0

    val idMessages: Map[Int, String] = Map(
      UNKNOWN -> "unknown"
    )
  }

  object Info extends ThrowableMessageType {
    val featureMask: Int = Features.INFO
    val categoryId: Int = featureMask
    val messageType: String = "I"

    // ID values
    val UNKNOWN = 0

    val idMessages: Map[Int, String] = Map(
      UNKNOWN -> "unknown"
    )
  }

  object Warning extends ThrowableMessageType {
    val featureMask: Int = Features.WARNING
    val categoryId: Int = featureMask
    val messageType: String = "W"

    // ID values
    val UNKNOWN = 0

    val idMessages: Map[Int, String] = Map(
      UNKNOWN -> "unknown"
    )
  }

  object Error extends ThrowableMessageType {
    val featureMask: Int = Features.ERROR
    val categoryId: Int = featureMask
    val messageType: String = "E"

    // ID values
    val UNKNOWN = 0
    val INTERNAL = 1
    val UNSUPPORTED_SCHEMA = 2
    val MISSING_REQUIRED_ARGUMENT = 3
    val DATA_QUALITY_CHECK_FAILED = 4

    val idMessages: Map[Int, String] = Map(
      UNKNOWN -> "unknown",
      INTERNAL -> "internal error",
      UNSUPPORTED_SCHEMA -> "unsupported schema",
      MISSING_REQUIRED_ARGUMENT -> "missing required argument",
      DATA_QUALITY_CHECK_FAILED -> "data quality check failed"
    )
  }

}
