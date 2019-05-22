name := "dataplatform_utils"
organization := "com.goibibo"
version := "2.4"
scalaVersion := "2.11.12"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions += "-target:jvm-1.8"

resolvers += "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
    "joda-time"         %   "joda-time"     % "2.10.1" % "provided",
    "org.slf4j"         %   "slf4j-api"     % "1.7.25" % "provided",
    "com.databricks"    %%  "spark-redshift" % "3.0.0-preview1" % "provided",
    "org.apache.zookeeper" %   "zookeeper" % "3.4.6" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.24" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % "provided",
    "com.databricks" % "dbutils-api_2.11" % "0.0.3",
    "com.jsuereth" %% "scala-arm" % "2.0",
    ("org.joda" % "joda-convert" % "2.1.2").
        exclude("com.google.guava","guava"),
    ("org.apache.hadoop" % "hadoop-aws" % "2.9.2" ).
      excludeAll( ExclusionRule(organization = "com.amazonaws")),
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.199" ,
    "com.amazonaws" % "aws-java-sdk-glue" % "1.11.199",
    "com.github.mjakubowski84" %% "parquet4s-core" % "0.4.0",
    "com.amazon.redshift" % "redshift-jdbc4" % "1.2.20.1043",
    "software.amazon.awssdk" % "glue" % "2.5.29",
    "software.amazon.awssdk" % "redshift" % "2.5.37",
    "org.scalatest" %% "scalatest" % "3.0.7" ,
    "com.github.pureconfig" %% "pureconfig" % "0.10.2",
    "com.jsuereth" %% "scala-arm" % "2.0",
    "io.delta" %% "delta-core" % "0.1.0" % "provided"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.spark.sql.delta.**" -> "com.databricks.sql.transaction.tahoe.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".so" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xsd" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".dtd" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".txt" => MergeStrategy.concat
  case PathList(ps @ _*) if ps.last endsWith "reference-overrides.conf" => MergeStrategy.concat
  case PathList(ps @ _*) if ps.last endsWith ".json" => MergeStrategy.concat
  case PathList(ps @ _*) if ps.last endsWith "customization.config" => MergeStrategy.concat
  case PathList(ps @ _*) if ps.last contains "Log" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
