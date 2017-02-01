package examples.fancy_numbers

import com.swoop.spark.records._
import com.swoop.spark.test.SparkSqlSpec
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel


class SparkTest extends ExampleSpec with SparkSqlSpec with TestNegative5To100 {

  lazy val dc = SimpleDriverContext(sc)
  lazy val jc = dc.jobContext(SimpleJobContext)
  lazy val ds = recordsDataset(-5 to 100, jc)
  lazy val records = ds.collect

  "in an integration test" - {
    implicit val env = new FlatRecordEnvironment()
    val sqlContext = sqlc
    import sqlContext.implicits._

    behave like fancyRecordBuilder(records, jc)

    "should build records with Spark" in {
      ds.count should be(105)
    }
    "should filter error records" in {
      ds.errorRecords.count should be(6)
    }
    "should extract data from records" in {
      ds.recordData.count should be(99)
    }
    "should extract issues" in {
      ds.allIssues.count should be(8)
      ds.errorIssues.count should be(6)
    }
    "should demonstrate issueCounts() output" in {
      ds.issueCounts.show(false)
    }
    "should demonstrate errorIssueCounts() output" in {
      ds.errorIssueCounts.show(false)
    }
    "should demonstrate messageCounts() output" in {
      ds.messageCounts.show(false)
    }
    "should demonstrate errorMessageCounts() output" in {
      ds.errorMessageCounts.show(false)
    }
    "should demonstrate errorDetailCounts() output" in {
      ds.errorIssues.errorDetailCounts().show
    }
    "should demonstrate unknownErrorDetailCounts() output" in {
      ds.errorIssues.unknownErrorDetailCounts("examples.fancy_numbers").show
    }
    "should demonstrate errorDetails() output" in {
      ds.errorIssues.errorDetails().show
    }
    "should demonstrate unknownErrorDetails() output" in {
      ds.errorIssues.unknownErrorDetails("examples.fancy_numbers").show
    }
  }

  def recordsDataset(numbers: Seq[Int], jc: JobContext): Dataset[FancyNumberRecord] = {
    val sqlContext = sqlc
    import sqlContext.implicits._
    sqlc.createDataset(numbers)
      .mapPartitions(inputs => Example.buildRecords(inputs, jc))
      .persist(StorageLevel.MEMORY_ONLY)
  }

}
