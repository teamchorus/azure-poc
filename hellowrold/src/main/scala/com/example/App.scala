package com.example

import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions._

object App {
  def main(args: Array[String]) {
    println("Vineet started the main thread!")
    val spark = SparkSession
      .builder()
      .appName("VineetSparkJob")
      .master("yarn")
      .getOrCreate()
    println("Hello World from Vineet!")

    // Create a DataFrame from a sequence of tuples
    val data = Seq(
      ("Sue", 32),
      ("Li", 3),
      ("Bob", 75),
      ("Heo", 13)
    )

    val df: Dataset[Row] = spark.createDataFrame(data).toDF("first_name", "age")

    // Show the initial DataFrame
    println("Initial DataFrame:")
    df.show()

    // Add a new column based on conditions
    val dfWithLifeStage = df.withColumn(
      "life_stage",
      when(col("age") < 13, "child")
        .when(col("age").between(13, 19), "teenager")
        .otherwise("adult")
    )

    // Show the updated DataFrame with the new column
    println("DataFrame with Life Stage:")
    dfWithLifeStage.show()

    // Save the DataFrame as a Parquet table (for later SQL queries)
    dfWithLifeStage.write.mode("overwrite").saveAsTable("some_people")

    // Query the table using Spark SQL
    spark.sql("SELECT * FROM some_people").show()

    spark.stop()
    println("Vineet stoped the spark session!")
  }

}
