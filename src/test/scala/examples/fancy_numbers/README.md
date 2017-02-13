# The Fancy Numbers Example

This example serves several purposes. It demonstrates the use of some intermediate Spark Records capabilities such as ignoring inputs, collecting custom metrics, throwing identifiable exceptions, etc. It also shows one simple approach for testing Spark Records-based builders.

## The example in action

There are two ways to see this example in action:

- Run `sbt test` and look at the output for [`LocalTest`](LocalTest.scala) and [`SparkTest`](SparkTest.scala). 

- Follow the interactive example in [this notebook](https://swoop-inc.github.io/spark-records/fancy_numbers_example.html).

## The code

The code in [Example.scala](Example.scala) has detailed comments.

## The tests

The tests are based on [ScalaTest](http://www.scalatest.org/) and use [FreeSpec](http://doc.scalatest.org/3.0.1/#org.scalatest.FreeSpec). The testing approach is optimized for a smooth development workflow based on a pattern which you can use in your own projects.

The idea is to start with fast local testing that does not depend on Spark and then expand that to full integration tests with Spark. To do this while staying [DRY](https://en.wikipedia.org/wiki/Don't_repeat_yourself), we create a shared test trait ([`TestNegative5To100`](TestNegative5To100.scala)), which we first use in the [`LocalTest`](LocalTest.scala) harness and later in the integration test harness ([`SparkTest`](SparkTest.scala)). For additional DRYness we make our traits and classes extend the same shared spec trait: [`ExampleSpec`](ExampleSpec.scala).
 
`SparkTest` uses an extremely simple Spark testing harness from the `spark-test-sugar` library we developed at [Swoop](https://www.swoop.com) back in the Spark 1.x days, which we have not had a chance to open-source yet. When you extend `SparkSqlSpec` you automatically get `sc` (`SparkContext`) and `sqlc` (`SQLContext`). That's all there is to it. You can use any Spark testing harness you choose.

**Tip:** If you are dealing with complex state in the Spark driver that your builder will use, it often makes sense to start testing with Spark from the very beginning as that will ensure you catch any dreaded `Task Not Serializable` situations immediately. 

Either way, `~testQuick` and `testOnly *MySuite* -- -z "my test"` are lifesavers in `sbt`.

Happy testing!
