---
layout: docs
---

# Spark Records by example

In this minimum viable example, we will use Spark to double numbers. Even a trivial example of Spark Records demonstrates the power of applying repeatable patterns for data processing.

As always, start with some imports.

```tut
import com.swoop.spark.records._
import org.apache.spark.sql.SparkSession
```

Define a case class for the output data, which is just a number in this case.

```tut
case class Number(n: Long)
```

Define the record envelope that will wrap the data. 

```tut
case class NumberRecord(
  features: Int,
  data: Option[Number] = None,
  source: Option[Long] = None,
  flight: Option[String] = None,
  issues: Option[Seq[Issue]] = None
) extends Record[Number, Long]
```

What you see here are the minimum required fields for records:

 - `features` is a bit mask containing various framework and user-provided flags for quick and efficient record categorization & filtering.

 - `data` contains our data. Some error records may have no data, e.g., if the error occurred before the data was created. Some may have data what was successfully built from the input but then determined to be invalid.

 - `source` identifies the input used to create the data. You have complete control over how to use this field. A common strategy is to store an ID uniquely identifying the source or, if there is no such thing and the source is large, e.g., in the case of large JSON input, only store the source with error records.

- `flight` is an ID that links together the output of one or more Spark jobs that are somehow related. In complex data processing scenarios, flight IDs are often associated with metadata related to the job, e.g., job parameters, cluster configuration, etc. If you don't manage flights explicitly, Spark Records will automatically provide a random UUIDv4 for each instance of `DriverContext`.

- `issues` supports the structured row-level logging automatically provided by Spark Records. We'll get into its details later.

Spark Records are extensible: add more fields if you need them. Timestamps, record IDs and schema versioning fields are common.

Now, create the Spark Records builder that will do the difficult job of multiplying the input numbers by 2.

```tut
case class Doubler(n: Long, override val jc: JobContext) extends RecordBuilder[Long, Number, NumberRecord, JobContext](n, jc) {
  def buildData: Option[Number] = Some(Number(2 * n))
  
  def dataRecord(data: Number, issues: Seq[Issue]): NumberRecord = 
    NumberRecord(features, Some(data), Some(n), jc.flight, issues)
  
  def errorRecord(issues: Seq[Issue]): NumberRecord = 
    NumberRecord(features, maybeData, Some(n), jc.flight, issues)
}
```

In the snippet above, `buildData` does all the work. It returns an `Option` because in the general case you may choose to not generate records for some input data. `dataRecord` and `errorRecord` are there to provide fine-grained control over how records are emitted. Ignore `JobContext` for now. It provides various services to the builder but we won't use any of them in this simple example.

We are ready to start Spark and prepare for transforming data.

```tut
val spark = SparkSession.builder().master("local").getOrCreate()
import spark.implicits._

val dc = SimpleDriverContext(spark)
val jc = dc.jobContext(SimpleJobContext)
```

The reason you see both a `DriverContext` and a `JobContext` is developer workflow optimization. If you can to construct a `JobContext` without needing `SparkSession`, `SparkContext` or `SQLContext` you can have very fast unit tests that are not slowed down by Spark initialization and overhead. It's the job of `DriverContext` to deal with Spark. It sets up variable broadcasting, registers accumulators, etc. That's why Spark Records integration tests start by creating a driver context.

Create a records dataset and force it to be evaluated by invoking the `count` action.

```tut
val records = spark.range(0, 10).flatMap(n => Doubler(n, jc).build)
assert(records.count == 10)
```

Hmm, we could have doubled numbers with a single line in Spark: `spark.range(0, 10).map(n => Number(2 * n))`. What have we gained from the dozen or so extra lines that Spark Records requires?

For starters, we've gained an automatic metrics collection capability.

```tut
jc.printStats()
```

Metrics are immediately available whether job execution succeeds of fails because they are implemented using Spark accumulators. You don't have to worry about the slowdown caused by chatting to a remote collection service or the complexity of having to query the data after the job completes. You also don't have to fret about what will happen to your remote collection endpoint if you suddenly started processing a complex job on a 1,000 node cluster. Collect your own metrics using `jc.inc()`.

Automatic metrics collection enables automatic data quality checks. In this case, we expect 10 inputs, no errors and no skipped inputs. (A skipped input is one where no record is emitted and no error is generated.)

```tut
jc.checkDataQuality(minInputs = 10, maxErrorRate = 0, maxSkippedRate = 0)
```

As expected, the data quality check passed. Had it failed, we would have gotten an exception.

Anyway, let's double check to make sure that we don't have any error records. 

That brings up the question of what is an error record. It's a record whose `features` has the bit for `Features.ERROR` set. (That happens to be the least significant bit, `1`). The simplest way to find the error records would to be scan all the data but there are more efficient ways to store Spark records that make error record identification very fast, e.g., through partitioning. So, how can Spark Records know the best way to look for the error records? The answer lies in _record environments_, which implicitly provide a hint to the framework without cluttering APIs with extra parameters.

In our simple example we did not use partitioning so we are in a flat record environment. Had we used partitioning, we'd create an implicit `PartitionedRecordEnvironment`.

```tut
implicit val env = new FlatRecordEnvironment()
assert(records.errorRecords.count == 0)
```

Spark Records extended the Dataset API with an `errorRecords` method, saving us from having to type:

```tut:silent
records.filter(r => (r.features & Features.ERROR) != 0)
```

Or, if the records were partitioned the way we do it at [Swoop](https://www.swoop.com):

```tut:fail:silent
records.filter('par_cat === "bad")
```

Let's take a look at the records schema.

```tut
records.printSchema
```

Everything is pretty simple until you get to `issues` where you see triple-nested arrays of issues, causes and stack trace elements. Don't worry, just as with `errorRecords()`, Spark Records provides enough sugar to make root cause analysis using `issues` data sweet. We'll see this in the next example.

If you peek at the records, you'll see they all share the same automatically-generated flight ID.

```tut
records.show(truncate = false)
```

If you want just the data and you are in a flat record environment, you could use `flatMap`.

```tut
records.flatMap(_.data).printSchema
```

Alternatively, you could use an implicit method enabled by the record environment that will work regardless of whether your data is flat or partitioned.

```tut
records.recordData.show()
```
```tut:invisible
spark.stop()
```

Bulletproof execution, row-level logging, automatic metrics collection and data quality checks don't matter much if the problems are so simple that input data is always valid and squeaky clean and that code is bug-free. Spark Records really shines in more complex real-world situations.

# Real world problems

Real-world data transformation problems are messy. Data may be dirty or invalid or come from untrusted sources. Some failures are to be expected and should be ignored. However, code complexity ensures there will occasionally be unexpected failures and those must be investigated swiftly. To demonstrate how Spark Records helps with these problems, we've included a more complex example as part of the test code of the framework in [`examples.fancy_numbers`](https://github.com/swoop-inc/spark-records/blob/master/src/test/scala/examples/fancy_numbers/). The best way to follow the example is through [this notebook](fancy_numbers_example.html).

# Root cause analysis

The example in the previous section outlines the typical root cause analysis workflow with Spark Records-based data:

1. Look for high-level problems in the metrics collected during job execution.

2. Get an overview of the issues across all records or just error records, depending on what type of problem you are investigating.

3. Partition issues into known/expected ones and unknown/unexpected ones.

4. Filter error records to the latter group and drill into error details of the unknown/unexpected issues.

5. Mitigate based on the findings.

6. When you have the time, modify your builder code to associate the previously unknown issues with issue IDs so that they become positively identifiable in the future.

Spark Records supports this workflow through a number of implicit extensions to `DataFrame` and `Dataset[A]`. You can find them in the `com.swoop.spark.records` [package object](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/records/package.scala).

In addition to using the Spark Records tooling, you can perform your own advanced root cause analysis directly on the data. In [this notebook](advanced_root_cause_analysis.html) you'll see this done with SparkSQL and Python.

# Testing

See the [testing documentation](https://github.com/swoop-inc/spark-records/blob/master/src/test/scala/examples/fancy_numbers/) for the fancy numbers example.

# Advanced topics

## Composite builders

In some situations data transformation requires producing more than one "row" of data for each input "row". Typically, this happens when the inputs have been grouped in some way. Spark Record supports this use case with composite builders:

- At the top level, create a builder that extends [`CompositeRecordBuilder`](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/records/CompositeRecordBuilder.scala).

- Implement `recordBuilder()` to construct a subclass of [`NestedRecordBuilder`](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/records/NestedRecordBuilder.scala), which will build one output record. A nested record builder is identical to `RecordBuilder` with the exception that it does not increment the `input.count` metric when `build()` is called.

- Implement `buildRecords()` where you break up the input data and call `buildPartition()` with each input to get a record. Behind the covers, `buildPartition()` calls `recordBuider()`.

## Build context

To enable simple yet flexible record building Spark Records includes three types of build context:

- [`DriverContext`](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/records/DriverContext.scala) that deals with Spark-related initialization.

- [`JobContext`](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/records/JobContext.scala), which provides metrics and flight tracking services for record building and, ideally, does not interact with Spark context directly in order to allow for fast Spark-less tests.

- The builder instance itself, which exposes the job context and provides APIs for accumulating feature flags and issues. In a complex use case, you'd probably want to break up record building code across several classes and/or modules. Rather than having to make these dependent on the exact type of builder you are using, it's better to have them depend on [`BuildContext`](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/records/BuildContext.scala), which is a clean trait that aggregates the services provided by a builder. One could argue that it is bad design to have a builder instance expose one public API for building records to its clients and another to objects it uses to build records. That may be true in theory but, in our experience, even with very sophisticated builders, the added complexity and overhead of a "more OO" design simply leads to more complex code and more boilerplate with no practical benefits.

# Best practices

## Data partitioning

Real-world production scenarios require the efficient separation of error records from records with valid data. That's best done through partitioning. All records where `(features & Features.ERROR) != 0` go to one partition and the rest go to one or more other partitions. Filtering data with Spark based on partitions is extremely fast: Spark only looks at the files from partition directories that are part of the query.

At [Swoop](https://www.swoop.com), all of our big production tables include at least two levels of partitioning.

The first is for time, using a `yyyymmddhhmm` format which enables fast range queries as well as variable persistence granularity. We use the standard name `par_ts` for the time partitioning column.

The second partitioning column, `par_cat`, is for the "category" of records. We choose the category based on follow-on query use cases and the natural skew of the data, which we also manage that through controlling Parquet file output size. We have reserved the category value `bad` for error records.

This follows another data pattern called Resilient Partitioned Tables (RPTs). By adopting a standardized approach, we get better framework and tooling support and we write less code.

## Idempotent jobs

98+% of Spark job failures we see are overwhelmingly related to some type of cloud I/O problem (S3 consistency violation, inaccessible database, etc.) or occasionally related to a core Spark failures, e.g., the driver inexplicably dying. These errors are transient; they self-correct. It doesn't make sense to spend any time performing root cause analysis if simply re-running the job would fix the problem...

...assuming, of course, that you can re-run the job without any ill consequence. To do this, the job has to be [idempotent](http://stackoverflow.com/questions/1077412/what-is-an-idempotent-operation). Next to correctness, idempotency may be the most desirable property of big data jobs because it makes reasoning about a job's side effects easy and it makes dealing with operational failures easy: 49 out of 50 times the problem will go away if you re-run the job. Better, have your scheduler or cluster controller automatically re-run the job.

While it is not easy to define exactly what makes a Spark job idempotent, it is very easy to point out two operations that definitely make jobs _not_ idempotent. (The following comments apply to I/O targets that are not transactional.) 

The first no-no operation is appending data. Without [ACIDity](https://en.wikipedia.org/wiki/ACID), an append operation that fails midway could leave your data in an inconsistent state. That's not the biggest problem, though. The biggest problem is that re-running the operation will likely make things worse, e.g., duplicate data. Instead of appending data, use updates in target sources that support them. If the target source does not support updates, e.g., Spark tables, you have to use partitioning and then simulate an update by overwriting a subset of the partitions. Alas, this doesn't always lead to a fully satisfying solution because...

...the second no-no operation is overwriting files that another job may depend on. When you start job output in overwrite mode, Spark deletes all files in the target path. Output can take a long time. Any jobs that depend on the previous data being there will fail. Worse, it is possible that a job could use partially-written data and produce incorrect results. 

The easiest way to solve the overwrite problem in Spark is by not having to solve it, which requires ensuring that for the duration of a job that will overwrite some data, no other job would attempt to use the data. If you cannot do that, you have to use some type of indirection, either by manually managing partitioning or through views. It's not trivial but it is well-worth the effort because it makes for a much more predictable and reliable operating environment. 

## Templatize RCA

If an automatic re-run of your job still causes a failure, it may be time for a human to get involved. To speed up root cause analysis it helps to create notebooks that take simple parameters such as the output path or the table name where the data is and then execute the types of root cause analysis queries you are interested in. This way you can kick off an investigation and come back in a minute to see all kinds of useful output.

## Run jobs from notebooks

We don't mean you should run jobs manually but that, however you kick off jobs, you should collect job-output in a notebook-like format and not in log files.

At [Swoop](https://www.swoop.com) we don't like spelunking into log files. While we write our jobs in libraries, we prefer to kick them off in a notebook because the notebook contains an easily consumable record of the job execution. Typically, we print the metrics collected during job execution and list all generated output files. It's amazing how quickly humans can notice patterns if relevant information is presented in an easy-to-consume manner. Log files can't do that and the Spark UI can't either.

## Catch job exceptions

In the [fancy numbers example](fancy_numbers_example.html), when the data quality check failed execution stopped before we could print the collected metrics. You'd save time and get helpful decision making context if you catch job failures and execute some number of information gathering steps regardless of whether the job is a success of failure. As mentioned in the previous point, at minimum we output collected metrics and generated files.

An easy way to do this in Scala is with `scala.util.Try`. If you are not familiar with it, see [this post](http://danielwestheide.com/blog/2012/12/26/the-neophytes-guide-to-scala-part-6-error-handling-with-try.html).

```scala
val jobResult = Try(runJob(driverContext))

driverContext.printStats

// You'll get the result of runJob() or the exception thrown by it
val jobOutput = jobResult.get
```

## RCA on job failure

If:
 
- you can catch job exceptions, and if 
- you can know whether this job run is the last allowed re-try of a previously failed run, and if 
- you have templatized RCA tooling, and if 
- you run your jobs from notebooks, then 
- you can also kick off root cause analysis automatically on job failure. 

This way, by the time a human gets the failure alert, all the initial information necessary for deciding how to mitigate has been assembled.

That's operational big data nirvana.

# Other goodies

## By key accumulation

The metrics collection in Spark Records is enabled by [`ByKeyAdditiveAccumulator`](https://github.com/swoop-inc/spark-records/blob/master/src/main/scala/com/swoop/spark/accumulators/ByKeyAdditiveAccumulator.scala), which can do a lot of tricks.

# API docs

You can find the latest API docs at [https://swoop-inc.github.io/spark-records/latest/api](latest/api).
