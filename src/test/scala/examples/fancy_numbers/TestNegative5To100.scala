package examples.fancy_numbers

import com.swoop.spark.records._


/** Shared test behavior trait used by [[LocalTest]] and [[SparkTest]]. */
trait TestNegative5To100 {
  this: ExampleSpec =>

  def fancyRecordBuilder(records: Iterable[FancyNumberRecord], jc: SimpleJobContext): Unit = {
    "fancy_numbers.Example.Builder" - {
      "with numbers from -5 to 100 should behave like a" - {
        "stats collector" in {
          // @formatter:off
          val stats = jc.stats
          stats("input.count")          should be(106)
          stats("numbers.even")         should be(51)
          stats("numbers.odd")          should be(50)

          stats("record.count")         should be(105)
          stats("record.error.count")   should be(6)   // -5..-1 & 100
          stats("record.skipped.count") should be(1)   // 0
          stats("record.data.count")    should be(99)  // 1..99

          stats("record.features.0")    should be(97)  // plain data
          stats("record.features.1")    should be(6)   // errors
          stats("record.features.2")    should be(2)   // warnings

          stats("issue.count")          should be(8)
          stats("issue.category.1")     should be(6)   // errors
          stats("issue.category.2")     should be(2)   // warnings
          stats("issue.id.1-1001")      should be(5)   // error FancyExample.Err.NEGATIVE_NUMBER
          stats("issue.id.2-1024")      should be(2)   // warning Features.QUALITY_CONCERN
        }
        "quality checker" in {
          val ex = the[ThrowableMessage] thrownBy {
            jc.checkDataQuality(minInputs=10, maxErrorRate=0.01, maxSkippedRate=0.001)
          }
          ex should have message "Too many errors. (inputs: 106, skipped: 1, errors: 6, withData: 99)"
          ex.idMessage should be("data quality check failed")
        }
        "perfect number record builder" in {
          val perfectNumbers = records.filter(_.data.exists(Seq(6, 28) contains _.n))
          forAll(perfectNumbers) { r =>
            inside(r) { case FancyNumberRecord(features, data, source, flight, maybeIssues) =>
              features                  should be(Features.WARNING)
              data.get.category         should be("perfect")
              source                    should be('empty)
              flight                    should be(jc.flight)
              inside(maybeIssues)   { case Some(issues) =>
                issues.length           should be(1)
                inside(issues.head) { case Issue(category, message, causes, id, details) =>
                  category              should be(Issue.Warning.categoryId)
                  id                    should be(Some(Features.QUALITY_CONCERN))
                  message               should be("W01024: rare number")
                  details               should be(Some("perfect, really?"))
                }
              }
            }
          }
        }
        // @formatter:on
      }
    }
  }

}
