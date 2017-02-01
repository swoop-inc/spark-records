package com.swoop.spark.accumulators

import com.swoop.spark.test.SparkSqlSpec
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._


class ByKeyAdditiveAccumulatorTest extends WordSpec with Matchers with SparkSqlSpec {

  "the accumulator" should {
    "sum values into a map" in {
      val acc = new ByKeyAdditiveAccumulator[String, Int]
      sc.register(acc)

      sc.parallelize(1 to 100)
        .foreach { value =>
          val category = if (value % 2 == 0) "even" else "odd"
          acc.add(category, 1)
        }

      acc.value.toMap should be(Map("odd" -> 50, "even" -> 50))
    }
  }

}
