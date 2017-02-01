package examples.fancy_numbers

import org.scalatest._


/** An aggregation of test traits used in our testing suites */
trait ExampleSpec extends FreeSpec with Matchers with Inspectors with Inside
