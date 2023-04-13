package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.types._

object MovieLen20MTest {
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  def main(args: Array[String]): Unit = {
    println("Hello world!")


    val spark = SparkSession.builder.appName("test gpu").master("local[16]")
//      .config("spark.executor.extraClassPath", "/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar")
//      .config("spark.driver.extraClassPath", "/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar")
//      .config("spark.rapids.sql.concurrentGpuTasks", 1)
//      .config("spark.driver.cores", 4)
      .config("spark.driver.memory", "6G")
//      .config("spark.executor.cores", 4)
      .config("spark.executor.memory", "12G")
//      .config("spark.task.cpus", 4)
//      .config("spark.executor.resource.gpu.amount", 1)
//      .config("spark.task.resource.gpu.amount", 0.25)
//      .config("spark.rapids.memory.pinnedPool.size", "2G")
//      .config("spark.sql.files.maxPartitionBytes", "512m")
//      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "16g")
      .getOrCreate()
    import spark.implicits._
    val customSchema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", FloatType, true),
      StructField("timestamp", LongType, true),
    ))
    val ratings = spark.read.format("csv").option("delimiter", ",").option("header","true").schema(customSchema)
      .load("/home/spark/ylv/data/ml-25m/ratings.csv")
//    println(ratings.show())
//    val ratings = spark.read.textFile("/home/spark/ylv/data/ml-10m/ml-10m/ratings.dat").map(parseRating).toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
//    val trainingPersist = training.persist()
//     Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

//    val model = als.fit(trainingPersist)
    val model = als.fit(training)
//    trainingPersist.unpersist()
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

    // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    // $example off$
    userRecs.show()
    movieRecs.show()
    userSubsetRecs.show()
    movieSubSetRecs.show()

    spark.stop()
  }
}


