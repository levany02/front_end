package main.scala

object ALSSkill2 {
  case class Rating(userId: Int, movieId: Int, rating: Float)

  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.recommendation.ALS
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._

    println("Hello world!")
    val spark = SparkSession.builder().master("local")
      .appName("als skill test")
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
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/data_2.events")
      .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/data_2.events")
      .getOrCreate()
    val df = spark.read.format("mongodb").load()
    val skill = df.select(col("userid"), col("behavior"), explode(col("skill")).alias("skill"))
//    println(skill.show())
    val ratingScore = spark.createDataFrame(Seq(("apply", 5), ("search", 4), ("view", 3))).toDF("behavior", "rating")
//    val levelScore = spark.createDataFrame(Seq(("Fresher", 1), ("Junior", 2), ("Middle", 3), ("Senior", 4), ("Leader", 5))).toDF("level", "levelId")
//    val levelDf = df.join(levelScore, df("level") === levelScore("level"), "left")
    val ratings = skill.join(ratingScore, skill("behavior") === ratingScore("behavior"), "left")
//    println(ratings.show())
    // String 2 Index
    val indexer = new StringIndexer()
      .setInputCols(Array("userid", "skill"))
      .setOutputCols(Array("user", "skillId"))
      .fit(ratings)
//
    val indexed = indexer.transform(ratings)
    val Array(training, test) = indexed.randomSplit(Array(1.0, 0.0))
    val rank = 5
    val epoches = 10
    val regNum = 0.001
    val als_skill = new ALS()
      .setMaxIter(epoches)
      .setRank(rank)
      .setRegParam(regNum)
      .setUserCol("user")
      .setItemCol("skillId")
      .setRatingCol("rating")
    val model_skill = als_skill.fit(training)

    val userRecsSkill = model_skill.recommendForAllUsers(10)
//    val userRecsLocation = model_location.recommendForAllUsers(4)
//    val userRecsItemLevel = model_level.recommendForAllUsers(5)
    indexed.select("userid", "user").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/skill/UserMapper.parquet")
    indexed.select("skill", "skillId").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/skill/SkillMapper.parquet")
//    indexed.select("location", "locationId").distinct.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/LocationMapper.parquet")
      userRecsSkill.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/skill/userRecsSkill.parquet")
//    userRecsLocation.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/userRecsLocation.parquet")
//    userRecsItemLevel.write.format("parquet").save("/home/spark/ylv/recommend_scala/alsModel/userRecsItemLevel.parquet")
    spark.stop()
  }
}
