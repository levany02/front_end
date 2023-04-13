package main.scala

object ALSNavi {
  case class Rating(userId: Int, movieId: Int, rating: Float)
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.ml.attribute.Attribute
    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.recommendation.ALS

    println("Hello world!")
    val spark = SparkSession.builder().master("local")
      .appName("MongoTest")
      .config("spark.driver.bindAddress", "127.0.0.1")
      //      .config("spark.executor.extraClassPath", "/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar")
      //      .config("spark.driver.extraClassPath", "/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar")
      //      .config("spark.rapids.sql.concurrentGpuTasks", 4)
      //      .config("spark.driver.memory", "4G")
      //      .config("spark.executor.cores", 4)
      //      .config("spark.executor.memory", "4G")
      //      .config("spark.task.cpus", 1)
      //      .config("spark.executor.resource.gpu.amount", 1)
      //      .config("spark.task.resource.gpu.amount", 0.25)
      //      .config("spark.rapids.memory.pinnedPool.size", "2G")
      //      .config("spark.sql.files.maxPartitionBytes", "512m")
      //      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.mongodb.read.connection.uri", "mongodb://10.122.6.17/vnw_job.events")
      .config("spark.mongodb.write.connection.uri", "mongodb://10.122.6.17/vnw_job.events")
      .getOrCreate()
//    val df = spark.read.format("mongodb").load()
    val df = spark.read.format("mongodb").option("spark.mongodb.database", "vnw_job").option("spark.mongodb.collection", "events").option("aggregation.pipeline", "[{'$match': {'eventTime': {'$gte': ISODate('2023-03-01T17:00:00Z'), '$lte': ISODate('2023-04-02T17:00:00Z')}}}, {'$project': {'entityId': 1, 'targetEntityId': 1, 'event': 1}}]").load()
    //    println(df.show(10))
    val ratingScore = spark.createDataFrame(Seq(("apply", 5), ("save", 4), ("view", 3))).toDF("behavior", "rating")
//    val levelScore = spark.createDataFrame(Seq(("Fresher", 1), ("Junior", 2), ("Middle", 3), ("Senior", 4), ("Leader", 5))).toDF("level", "levelId")
//    val levelDf = df.join(levelScore, df("level") === levelScore("level"), "left")
    val ratings = df.join(ratingScore, df("event") === ratingScore("behavior"), "left")

    // String 2 Index
    val indexer = new StringIndexer()
      .setInputCols(Array("entityId", "targetEntityId"))
      .setOutputCols(Array("user", "item_id"))
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
//
//    val als_item_name = new ALS()
//      .setMaxIter(5)
//      .setRank(5)
//      .setRegParam(0.01)
//      .setUserCol("user")
//      .setItemCol("item_name")
//      .setRatingCol("rating")
//    val model_item_name = als_item_name.fit(training)
//
//    val als_location = new ALS()
//      .setMaxIter(5)
//      .setRank(5)
//      .setRegParam(0.01)
//      .setUserCol("user")
//      .setItemCol("locationId")
//      .setRatingCol("rating")
//    val model_location = als_location.fit(training)
//
//    val als_level = new ALS()
//      .setMaxIter(5)
//      .setRank(5)
//      .setRegParam(0.01)
//      .setUserCol("user")
//      .setItemCol("levelId")
//      .setRatingCol("rating")
//    val model_level = als_level.fit(training)

//    val userRecsItemId = model_item_id.recommendForAllUsers(40)
//    val userRecsItemName = model_item_name.recommendForAllUsers(20)
//    val userRecsLocation = model_location.recommendForAllUsers(4)
//    val userRecsItemLevel = model_level.recommendForAllUsers(5)
//    indexed.select("userid", "user").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/UserMapper.parquet")
//    indexed.select("jobId", "item_id").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/ItemIdMapper.parquet")
//    indexed.select("job", "item_name").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/ItemNameMapper.parquet")
//    indexed.select("location", "locationId").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/LocationMapper.parquet")
//    userRecsItemId.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/userRecsItemId.parquet")
//    userRecsItemName.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/userRecsItemName.parquet")
//    userRecsLocation.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/userRecsLocation.parquet")
//    userRecsItemLevel.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/userRecsItemLevel.parquet")
    spark.stop()
  }
}
