package main.scala

//$SPARK_HOME/bin/spark-shell --class main.scala.MongoTest --jars "./target/scala-2.12/hello2_2.12-0.1.0-SNAPSHOT.jar,/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar,/home/spark/spark/jars/mongo-spark-connector/bson-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/bson-record-codec-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongodb-driver-core-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongodb-driver-sync-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongo-spark-connector_2.12-10.1.1.jar"

object MongoTest {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    println("Hello world!")
    val spark = SparkSession.builder().master("local")
      .appName("MongoTest")
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
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1")
      .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1")
      .getOrCreate()
    val df = spark.read.format("mongodb").option("database", "ecom_ur").option("collection", "events").load()

    println(df.show(10))
    spark.stop()
  }
}
