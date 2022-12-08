// Databricks notebook source
import java.io.File
import org.apache.spark.sql.functions._
import spark.implicits._   

var pathToFiles = "/FileStore/data/airbnb/"
val nameReviews = dbutils.widgets.get("fileName")
pathToFiles = pathToFiles + nameReviews

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

def preprocessesReviews(pathToFile: String, city: String): DataFrame ={
  var reviewsAirbnb = spark.read.format("csv").option("header", "true").option("sep", ",").load(pathToFile)
  reviewsAirbnb = reviewsAirbnb
    .withColumn("city", lit(city))
    .withColumn("listing_id", col("listing_id").cast(IntegerType))
    .withColumnRenamed("id","reviews_id")
    .withColumn("reviews_id", col("reviews_id").cast(IntegerType))
  reviewsAirbnb
}


// COMMAND ----------

if (fileIsExists(nameReviews)) {
  val reviewsDF = preprocessesReviews(pathToFiles+nameReviews, "London")
  display(reviewsDF)
}

// COMMAND ----------


