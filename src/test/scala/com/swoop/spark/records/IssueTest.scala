package com.swoop.spark.records

import org.scalatest._


class IssueTest extends WordSpec with Matchers {
  "issues package object" should {
    "expose Error" which {
      "creates an error from a Throwable" in {
        def createException = new IllegalArgumentException("bad argument")
        val result = Issue.from(createException).head

        result.category shouldBe Issue.Error.categoryId
        result.id shouldBe None
        result.message shouldBe "java.lang.IllegalArgumentException: bad argument"
        result.details shouldBe None
        result.causes.map(_.length).get shouldBe 1

        val cause = result.causes.get.head
        cause.message shouldBe "bad argument"
        cause.stack.length should be > 1

        val elem = cause.stack.head
        elem.className should include("IssueTest")
        elem.methodName shouldBe "createException$1"
        elem.fileName shouldBe "IssueTest.scala"
        elem.lineNumber shouldBe 10 // line number of createException() above
      }
    }
    "expose Warning" which {
      "creates a warning from a message" in {
        val result = Issue(Issue.Warning.categoryId, "beware")

        result.category shouldBe Issue.Warning.categoryId
        result.id shouldBe None
        result.message shouldBe "beware"
        result.details shouldBe None
        result.causes shouldBe None
      }
    }
  }
}
