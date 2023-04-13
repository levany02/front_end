package main.scala

object ALSImprove {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.ml.attribute.Attribute
    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.recommendation.ALS

    println("Hello world!")
    val spark = SparkSession.builder().master("local")
      .appName("MongoTest")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/data_1.events")
      .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/data_1.events")
      .getOrCreate()
    val df = spark.read.format("mongodb").load()
    //    println(df.show(10))
    val ratingScore = spark.createDataFrame(Seq(("apply", 5), ("search", 4), ("like", 4), ("view", 3), ("ignore", 2), ("hated", 1), ("dislike", 1))).toDF("behavior", "rating")
    val levelScore = spark.createDataFrame(Seq(("Fresher", 1), ("Junior", 2), ("Middle", 3), ("Senior", 4), ("Leader", 5))).toDF("level", "levelId")
    val levelDf = df.join(levelScore, df("level") === levelScore("level"), "left")
    val ratings = levelDf.join(ratingScore, levelDf("behavior") === ratingScore("behavior"), "left")
    // String 2 Index
    val indexer = new StringIndexer()
      .setInputCols(Array("userid", "jobId", "job", "location"))
      .setOutputCols(Array("user", "item_id", "item_name", "locationId"))
      .fit(ratings)

    val indexed = indexer.transform(ratings)
    val Array(training, test) = indexed.randomSplit(Array(1.0, 0.0))

    val als_item_id = new ALS()
      .setMaxIter(5)
      .setRank(5)
      .setRegParam(0.01)
      .setUserCol("user")
      .setItemCol("item_id")
      .setRatingCol("rating")
    val model_item_id = als_item_id.fit(training)
    val res = model_item_id.userFactors
    val res1 = model_item_id.itemFactors
    res.toDF().show()
  }
}
