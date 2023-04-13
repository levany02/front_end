package main.scala

//$SPARK_HOME/bin/spark-shell --class main.scala.EsTest --jars "./target/scala-2.12/hello2_2.12-0.1.0-SNAPSHOT.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/commons-logging-1.1.1.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/elasticsearch-spark-30_2.12-8.6.2.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/jaxb-api-2.3.1.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/protobuf-java-2.5.0.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/scala-library-2.12.8.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/scala-reflect-2.12.8.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/slf4j-api-1.7.6.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/spark-catalyst_2.12-3.2.3.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/spark-core_2.12-3.2.3.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/spark-sql_2.12-3.2.3.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/spark-streaming_2.12-3.2.3.jar,/home/spark/spark/jars/elasticsearch-spark-30_2-8_6/spark-yarn_2.12-3.2.3.jar"

object EsTest {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    println("Hello world!")

    val spark = SparkSession.builder().master("local[*]").appName("Es Test")
      .config("es.index.auto.create", "true")
      .config("spark.es.resource", "user_recs_1")
      .getOrCreate()

    val df = spark.read.format("es").load("user_recs_1")
    df.write.format("es").save("user_recs_2")

    println("Bulk Es successful.")
  }
}
