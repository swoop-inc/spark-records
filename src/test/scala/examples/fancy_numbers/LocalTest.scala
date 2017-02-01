package examples.fancy_numbers

import com.swoop.spark.records._


class LocalTest extends ExampleSpec with TestNegative5To100 {

  lazy val jc = SimpleJobContext()
  lazy val records = Example.buildRecords(-5 to 100, jc).toArray

  behave like fancyRecordBuilder(records, jc)

}
