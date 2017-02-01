---
layout: home
title: "Home"
section: "Home"
---

## Spark Records

Spark Records is a data processing pattern with an associated lightweight, dependency-free framework for [Apache Spark v2+][1] that enables:

1. **Bulletproof data processing with Spark**  
	Your jobs will never unpredictably fail midway due to data transformation bugs. Spark records give you predictable failure control through instant data quality checks performed on metrics automatically collected during job execution, without any additional querying.

2. **Automatic row-level structured logging**  
	Exceptions generated during job execution are automatically associated with the data that caused the exception, down to nested exception causes and full stack traces. If you need to reprocess data, you can trivially and efficiently choose to only process the failed inputs.

3. **Lightning-fast root cause analysis**  
	Get answers to any questions related to exceptions or warnings generated during job execution directly using SparkSQL or your favorite Spark DSL. Would you like to see the top 5 issues encountered during job execution with example source data and the line in your code that caused the problem? [You can](docs.html#root-cause-analysis).

Spark Records has been tested with petabyte-scale data at [Swoop][2]. The library was extracted out of Swoop's production systems to share with the Spark community.

See the [documentation][3] for more information.

## Who are Spark Records for?

Spark Records are for busy data engineers and data scientists who have to deal with complex data munging and/or unreliable/dirty data.

Spark Records are a data pattern. You can use it from [any programming language](docs.html#advanced-root-cause-analysis). Further, if your schema follows the pattern, the root cause analysis tooling in this framework, which is built in Scala, can be applied to your data even if the data has been produced using a different language. In other words, you don't have to use Scala to take advantage of much of the value that Spark Records bring.

## Overview

Records provide a structured envelope around your data. The contract between the envelope and the framework code is what enables the magic of Spark Records. Defining records is easy:

```scala
import com.swoop.spark.records._

case class MyData(/* whatever your data needs */)

case class MyDataRecord(
  features: Int,                     // features enable fast categorization
  data: Option[MyData] = None,       // this is your data, whatever you need
  source: Option[MyInput] = None,    // the source enables data provenance tracking
  flight: Option[String] = None,     // related jobs are part of the same flight
  issues: Option[Seq[Issue]] = None  // this is the "row-level log"
) extends Record[MyData, MyInput]
```

The envelope can be extended to include other fields. Because most of the envelope values, other than `data`, are either the same or `null`, the envelope has almost no storage overhead (because of [run-length encoding][4] in data storage formats such as [Parquet][5]). In columnar storage formats, the envelope also has essentially no query overhead.

The idea behind Spark Records is that users of records don't even know the records are there because you can expose just the data to them using views in SparkSQL or directly, in a manner that's independent of how the data is stored (flat or not, partitioned or not, etc.). 

Building records involves implementing three methods: one to create your data, one to wrap it in a record and one to create an error record. The last two are usually one liners, as shown below:

```scala
case class Builder(input: MyInput, override val jc: JobContext)
  extends RecordBuilder[MyInput, MyData, MyDataRecord, JobContext](input, jc) {
	
  def buildData: Option[MyData] = { /* your business logic here */ }
	
  def dataRecord(data: MyData, issues: Seq[Issue]): MyDataRecord =
    MyDataRecord(features, Some(data), None, jc.flight, issues)
	
  def errorRecord(issues: Seq[Issue]): MyDataRecord =
    MyDataRecord(features, maybeData, Some(input.toString), jc.flight, issues)
	
}
```

That's it. This is all you have to do to get most of the benefits of Spark Records: bulletproof exception management, automatic metrics collection and automatic data quality checks. 

```scala
jobContext.checkDataQuality(
  minInputs     = 1000000,   // fail if fewer than 1 million inputs
  maxErrorRate  = 0.00001,   // fail if more than 1 in 10,000 errors
  maxSkippedRate= 0.01       // fail if more than 1 in 100 skipped inputs
)
```

Of course, if you are dealing with complex problems, you can also take advantage of custom metrics, additional row-level logging features and advanced state management. 

Following the Spark Records data pattern really pays off when jobs fail due to errors and you have to perform root cause analysis. See the [documentation][6] for more information.

## Installation

Just add the following to your `libraryDependencies` in SBT:

```scala
resolvers += Resolver.bintrayRepo("swoop-inc", "maven")

libraryDependencies += "com.swoop" %% "spark-records" % "<version>"
```

You can find all released versions [here](https://github.com/swoop-inc/spark-records/releases).

## Community

Contributions and feedback of any kind are welcome. 

Spark Records is maintained by [Sim Simeonov][7] and the team at [Swoop][8]. 

Special thanks to [Reynold Xin][9] and [Michael Armbrust][10] for many interesting conversations about better ways to use Spark. 


[1]:	https://spark.apache.org/ "Apache Spark"
[2]:	https://www.swoop.com
[3]:	docs.html "Spark Records Documentation"
[4]:	https://en.wikipedia.org/wiki/Run-length_encoding
[5]:	https://github.com/Parquet/parquet-format/blob/master/Encodings.md
[6]:	docs.html "Spark Records Documentation"
[7]:	https://github.com/ssimeonov "Simeon Simeonov"
[8]:	https://www.swoop.com
[9]:	https://github.com/rxin
[10]:	https://github.com/marmbrus
