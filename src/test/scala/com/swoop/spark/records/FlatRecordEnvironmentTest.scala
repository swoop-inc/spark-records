package com.swoop.spark.records

import org.scalatest._


class FlatRecordEnvironmentTest extends WordSpec with Matchers {
  "a flat record environment" should {
    "validate custom data fields" which {
      "allows custom data fields that are subset of custom record fields" in {
        val env = FlatRecordEnvironment(
          customRecordFields = "a" :: "b" :: Nil,
          customDataFields = "a" :: Nil
        )

        env.allRecordFields should be(Seq("features", "data", "source", "flight", "issues", "a", "b"))
      }
      "throws an exception when custom data fields are not a subset of custom record fields" in {
        val ex = the[Exception] thrownBy {
          FlatRecordEnvironment(
            customRecordFields = "a" :: "b" :: Nil,
            customDataFields = "c" :: "a" :: "d" :: Nil
          )
        }

        ex should have message "customDataFields include the following fields not part of customRecordFields: c, d"
      }
    }
  }
}
