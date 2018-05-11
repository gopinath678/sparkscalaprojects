name := "SparkKafkaApp"

version := "1.0"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.0"
