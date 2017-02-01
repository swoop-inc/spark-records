package examples.fancy_numbers

import com.swoop.spark.records._

/** Let's create a simple but not simplistic example of data processing with Spark.
  *
  * We have to categorize integers in the range [0, 100] in the following way:
  *
  * - Numbers outside of [0, 100] are errors.
  *
  * - 0s should be ignored.
  *
  * - Prime numbers are categorized as `prime`.
  *
  * - Perfect numbers, those that are equal to the sums of their factors, e.g.,
  * 6 = 3 + 2 + 1, are categorized as `perfect`.
  *
  * - 13 is categorized as `bakers-dozen`
  *
  * - Everything else is categorized as `even` or `odd`.
  *
  * This example is simple because the inputs are integers but it is not simplistic
  * because we have to deal with errors and inputs that should not produce any output.
  * Further, just to make things a little more interesting, we'll introduce a bug in our code.
  *
  * Here is how we'd like to represent our output data.
  *
  * @param n        the input integer
  * @param category the integer's category
  */
case class FancyNumber(n: Int, category: String)


/** Wrapping our data with a record is simple: we extend `Record[OurData, OurInput]` and
  * then provide defaults. Below you see the minimum set of fields you need in a record.
  * You can add more fields if you need more information in the wrapper. Common examples include
  * adding a timestamp, a record ID or schema versioning information.
  *
  * @see [[com.swoop.spark.records.Record]]
  * @see [[com.swoop.spark.records.Issue]]
  * @param features A bit mask that allows records to be easily filtered
  * @param data     The data the record wraps. May be empty in the case of some error records.
  * @param source   The source of the data. This is how we keep track of data provenance.
  *                 A common optimization is to only include the source for error records.
  * @param flight   Several jobs that work together to produce a result share the same flight ID.
  *                 How you choose to organize this is up to you. The ID is often a UUIDv4.
  * @param issues   The [[issues]] field contains the record-level "log file".
  */
case class FancyNumberRecord(
  features: Int,
  data: Option[FancyNumber] = None,
  source: Option[Int] = None,
  flight: Option[String] = None,
  issues: Option[Seq[Issue]] = None
) extends Record[FancyNumber, Int]


/** This is just a holder for our example code */
object Example {

  /** A record builder for a simple use case like the one we are dealing with should
    * extend [[RecordBuilder]].
    *
    * [[JobContext]] provides state management and other services during building.
    * In this example, we'll use it collect some custom statistics.
    *
    * @see [[RecordBuilder]]
    * @see [[JobContext]]
    * @see [[BuildContext]]
    * @param n  the input integer to categorize
    * @param jc the job context
    */
  case class Builder(n: Int, override val jc: JobContext)
    extends RecordBuilder[Int, FancyNumber, FancyNumberRecord, JobContext](n, jc) {

    /** This is the main method we have to implement. Here we build an
      * [[examples.fancy_numbers.FancyNumber]] from the input.
      *
      * @return the record data generated for this input or `None`
      */
    def buildData: Option[FancyNumber] = {
      // Use throwError to throw _identifiable exceptions_, ones you can easily find
      // after the job has finished executing
      if (n < 0 || n > 100) throwError("input outside [0, 100]", n.toString, Err.OUT_OF_RANGE)

      // jc.inc() is how we can collect custom metrics during job execution
      val stat = if (n % 2 == 0) "even" else "odd"
      jc.inc(s"numbers.$stat")

      if (n == 0) None // returning None will skip this input and generate no record
      else Some(FancyNumber(n, category))
    }

    /** Returns a record with valid data.
      * This method is a hook to allow you to create your own custom records.
      * It uses the state that has been accumulated during the building of the record's data,
      * namely, `features` and `issues`.
      *
      * When everything is going well, we save on storage by not saving the source of the data.
      *
      * @param data   the data produced in [[buildData]]
      * @param issues the issues collected during the building process
      * @return A valid record with data
      */
    def dataRecord(data: FancyNumber, issues: Seq[Issue]): FancyNumberRecord =
      FancyNumberRecord(features, Some(data), None, jc.flight, issues)

    /** Returns an error record.
      * This method is called if the record will contain an error.
      *
      * Depending on whether the error occurred during or after [[buildData]], the resulting
      * record may or may not have `data`. In our case `maybeData` will always be `None`
      * because there is no code path where an error is generated after [[buildData]].
      *
      * @param issues the issues collected during the building process, at least one of
      *               which will be an error
      * @return A valid error record
      */
    def errorRecord(issues: Seq[Issue]): FancyNumberRecord =
      FancyNumberRecord(features, maybeData, Some(n), jc.flight, issues)

    /** Returns the category of the input number. */
    def category: String =
      if (primesTo100(n)) "prime"
      else if (n == 6 || n == 28) {
        // warn(), info() and debug() let you store messages inside the `issues` collection
        // associated with each record. Debug messages are particularly useful during
        // development. Features.QUALITY_CONCERN is one of the predefined record flags.
        warn("rare number", "perfect, really?", Features.QUALITY_CONCERN)
        "perfect"
      }
      else if (n == 13) "bakers-dozen"
      else if (n % 2 == 0) "even"
      else "odd"

  }

  /** One of the key principles of bulletproof big data processing is controlling failure modes
    * and ensuring that errors can be quickly separated in appropriate buckets, e.g., known vs.
    * unknown, safely ignorable vs. something one has to act on, etc.
    *
    * In Spark records, the first step in doing this involves associating IDs with known/expected
    * error types.
    */
  object Err {
    /** The ID of the error raised when we get an out-of-range number. */
    val OUT_OF_RANGE = 1001
  }

  /** Integers IDs are great for big data processing but not great for comprehension.
    * Fear not: you can associated descriptive messages with error (and warning, info, debug)
    * IDs in the following manner. The Spark Records root cause analysis framework will
    * automatically make these available without adding them to the data.
    */
  Issue.addMessagesFor(Issue.Error, Map(
    Err.OUT_OF_RANGE -> "number out of range"
  ))

  /** This little bit of API sugar makes it easier to plug a builder into Spark's RDD and
    * Dataset APIs. It is not required but makes code simpler & cleaner.
    */
  def buildRecords[Ctx <: JobContext](inputs: TraversableOnce[Int], jc: Ctx)
  : Iterator[FancyNumberRecord] =
    inputs.flatMap(Builder(_, jc).build).toIterator

  /** A simple prime number algorithm that builds a boolean array telling us whether a
    * number is prime. We've introduced an off-by-one error in the implementation on
    * purpose to show how Spark Records deals with unexpected failures (as opposed to
    * out of range input, which is an expected failure).
    *
    * @see http://alvinalexander.com/text/prime-number-algorithm-scala-scala-stream-class
    */
  private lazy val primesTo100 = {
    def primeStream(s: Stream[Int]): Stream[Int] = Stream.cons(s.head, primeStream(s.tail filter {
      _ % s.head != 0
    }))

    val arr = Array.fill(100)(false) // bug: should be 101 because of 0-based indexing
    primeStream(Stream.from(2)).takeWhile(_ <= 100).foreach(arr(_) = true)
    arr
  }

}
