package main.scala

import org.apache.spark.sql.SparkSession

object GPUTest {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val spark = SparkSession.builder.appName("test gpu").master("local[*]")
      .config("spark.executor.extraClassPath","/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar")
      .config("spark.driver.extraClassPath","/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar")
      .config("spark.rapids.sql.concurrentGpuTasks", 1)
      .config("spark.driver.memory", "4G")
      .config("spark.executor.cores", 4)
      .config("spark.executor.memory", "4G")
      .config("spark.task.cpus", 4)
      .config("spark.executor.resource.gpu.amount", 1)
      .config("spark.task.resource.gpu.amount", 0.25)
      .config("spark.rapids.memory.pinnedPool.size", "2G")
      .config("spark.sql.files.maxPartitionBytes", "512m")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .getOrCreate()
    println("Create sparkSession success.")
  }
}
