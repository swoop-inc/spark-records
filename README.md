# Spark Records

Spark Records is a data processing pattern with an associated lightweight, dependency-free framework for [Apache Spark v2+](https://spark.apache.org/) that enables:

1. **Bulletproof data processing with Spark**  
	Your jobs will never unpredictably fail midway due to data transformation bugs. Spark records give you predictable failure control through instant data quality checks performed on metrics automatically collected during job execution, without any additional querying.

2. **Automatic row-level structured logging**  
	Exceptions generated during job execution are automatically associated with the data that caused the exception, down to nested exception causes and full stack traces. If you need to reprocess data, you can trivially and efficiently choose to only process the failed inputs.

3. **Lightning-fast root cause analysis**  
	Get answers to any questions related to exceptions or warnings generated during job execution directly using SparkSQL or your favorite Spark DSL. Would you like to see the top 5 issues encountered during job execution with example source data and the line in your code that caused the problem? [You can](https://swoop-inc.github.io/spark-records/docs.html#root-cause-analysis).

Spark Records has been tested with petabyte-scale data at [Swoop](https://www.swoop.com). The library was extracted out of Swoop's production systems to share with the Spark community.

See the [documentation](https://swoop-inc.github.io/spark-records/) for more information or watch the [Spark Summit talk](https://spark-summit.org/east-2017/events/bulletproof-jobs-patterns-for-large-scale-spark-processing/).

<div style="text-align: center;"><iframe src="//www.slideshare.net/slideshow/embed_code/key/TSOLI6UGKLFZE" width="595" height="374" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe></div>

## Installation

Just add the following to your `libraryDependencies` in SBT:

```scala
resolvers += Resolver.bintrayRepo("swoop-inc", "maven")

libraryDependencies += "com.swoop" %% "spark-records" % "<version>"
```

You can find all released versions [here](https://github.com/swoop-inc/spark-records/releases).

## Community

Contributions and feedback of any kind are welcome.

Spark Records is maintained by [Sim Simeonov](https://github.com/ssimeonov) and the team at [Swoop](https://www.swoop.com).

Special thanks to [Reynold Xin](https://github.com/rxin) and [Michael Armbrust](https://github.com/marmbrus) for many interesting conversations about better ways to use Spark.

## License

`spark-records` is Copyright &copy; 2017 [Simeon Simeonov](https://about.me/simeonov) and [Swoop, Inc.](https://www.swoop.com) It is free software, and may be redistributed under the terms of the LICENSE.
