name := "DataMart"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" 
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0" 
libraryDependencies += "com.clickhouse" % "clickhouse-jdbc" % "0.6.4"
libraryDependencies += "net.sf.py4j" % "py4j" % "0.10.9.5" 
