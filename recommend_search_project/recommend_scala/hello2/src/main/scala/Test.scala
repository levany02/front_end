package main.scala

object Test {
  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017/ecom_ur.events")
      .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017/ecom_ur.events")
      .config("es.index.auto.create", "true")
      .config("es.httpHosts", "127.0.0.1:9200")
      .getOrCreate()
    val df = spark.createDataFrame(Seq(("user1", "item1"), ("user2", "item2"))).toDF("user", "item")
    df.write.format("es").save("test_index_1")
    println(df.show())
  }
}
