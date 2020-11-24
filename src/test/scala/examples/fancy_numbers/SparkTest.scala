package examples.fancy_numbers

import com.swoop.spark.SparkSessionTestWrapper
import com.swoop.spark.records._
import org.apache.spark.sql.{Dataset}
import org.apache.spark.storage.StorageLevel


object transforms {
  def errorRecords()(ds: Dataset[FancyNumberRecord]) = {
    ds.filter(r => (r.features & Features.ERROR) != 0)
  }
}


class SparkTest extends ExampleSpec with SparkSessionTestWrapper with TestNegative5To100 {

  val sc = spark.sparkContext
  lazy val dc = SimpleDriverContext(sc)
  lazy val jc = dc.jobContext(SimpleJobContext)
  lazy val ds = recordsDataset(-5 to 100, jc)
  lazy val records = ds.collect

  "in an integration test" - {
    implicit val env = FlatRecordEnvironment()
    import spark.implicits._

    behave like fancyRecordBuilder(records, jc)

    "should build records with Spark" in {
      ds.count should be(105)
    }
    "should filter error records" in {
      ds.transform(transforms.errorRecords()).count() should be(6)
//      ds.errorRecords.count should be(6)
//      ds.filter(r => (r.features & Features.ERROR) != 0).count should be(6)
    }
    "should extract data from records" in {
//      ds.recordData.count should be(99)
      ds.filter(r => (r.features & Features.ERROR) == 0).count should be(99)
    }
    "should extract issues" in {
      val df = new RootCauseAnalysis(ds.filter(_.issues.isDefined).toDF(), env).issues
      df.show()
//      ds.allIssues.count should be(8)
//      ds.errorIssues.count should be(6)
    }
//    "should demonstrate issueCounts() output" in {
//      ds.issueCounts.show(false)
//    }
//    "should demonstrate errorIssueCounts() output" in {
//      ds.errorIssueCounts.show(false)
//    }
//    "should demonstrate messageCounts() output" in {
//      ds.messageCounts.show(false)
//    }
//    "should demonstrate errorMessageCounts() output" in {
//      ds.errorMessageCounts.show(false)
//    }
//    "should demonstrate errorDetailCounts() output" in {
//      ds.errorIssues.errorDetailCounts().show
//    }
//    "should demonstrate unknownErrorDetailCounts() output" in {
//      ds.errorIssues.unknownErrorDetailCounts("examples.fancy_numbers").show
//    }
//    "should demonstrate errorDetails() output" in {
//      ds.errorIssues.errorDetails().show
//    }
//    "should demonstrate unknownErrorDetails() output" in {
//      ds.errorIssues.unknownErrorDetails("examples.fancy_numbers").show
//    }
  }

  def recordsDataset(numbers: Seq[Int], jc: JobContext): Dataset[FancyNumberRecord] = {
    import spark.implicits._
//    implicit val fancyNumberRecordEncoder = org.apache.spark.sql.Encoders.kryo[FancyNumberRecord]
    spark.createDataset(numbers)
      .mapPartitions(inputs => Example.buildRecords(inputs, jc))
      .persist(StorageLevel.MEMORY_ONLY)
  }

}
