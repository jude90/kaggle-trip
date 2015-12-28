name := "kaggle-trip"

version := "1.0"

scalaVersion := "2.10.6"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"

libraryDependencies += "org.apache.hadoop" % "zookeeper" % "3.3.1"


libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.1"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.2.0"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.
absolutePath+"/.m2/repository"
