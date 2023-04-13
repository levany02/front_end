cd /home/spark/ylv/recommend_search_project/recommend_scala/hello2/
sbt package
rm -rf /home/spark/ylv/recommend_scala
$SPARK_HOME/bin/spark-shell --class main.scala.ALSExample --jars "./target/scala-2.12/hello2_2.12-0.1.0-SNAPSHOT.jar,/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar,/home/spark/spark/jars/mongo-spark-connector/bson-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/bson-record-codec-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongodb-driver-core-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongodb-driver-sync-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongo-spark-connector_2.12-10.1.1.jar"
$SPARK_HOME/bin/spark-shell --class main.scala.ALSSkill --jars "./target/scala-2.12/hello2_2.12-0.1.0-SNAPSHOT.jar,/home/spark/Downloads/rapids-4-spark_2.12-23.02.0.jar,/home/spark/spark/jars/mongo-spark-connector/bson-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/bson-record-codec-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongodb-driver-core-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongodb-driver-sync-4.8.2.jar,/home/spark/spark/jars/mongo-spark-connector/mongo-spark-connector_2.12-10.1.1.jar"
/home/spark/miniconda3/envs/web/bin/python /home/spark/ylv/recommend_search_project/recommend/load_model_1.py


