// Databricks notebook source
// MAGIC %md
// MAGIC # Processing listings.csv
// MAGIC 
// MAGIC ...

// COMMAND ----------

import java.io.File
import org.apache.spark.sql.functions._
import spark.implicits._   

val pathToFiles = "/FileStore/data/airbnb/"
val nameListings = dbutils.widgets.get("fileName")


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


def preprocessingListings(pathToFile: String, city: String): (DataFrame, DataFrame) ={
  var listingsAirbnb = spark.read.format("csv").option("header", "true").option("sep", ",").load(pathToFile)
  listingsAirbnb = listingsAirbnb
    .withColumn("city", lit(city))
    .withColumn("scraped_year", year(col("last_scraped")))
    .withColumn("calculated_host_listings_count_private_rooms", col("calculated_host_listings_count_private_rooms").cast(IntegerType))
    .withColumn("calculated_host_listings_count_entire_homes", col("calculated_host_listings_count_entire_homes").cast(IntegerType))
    .withColumn("calculated_host_listings_count_shared_rooms", col("calculated_host_listings_count_shared_rooms").cast(IntegerType))
    .withColumn("calculated_host_listings_count", col("calculated_host_listings_count").cast(IntegerType))
    .withColumn("minimum_maximum_nights", col("minimum_maximum_nights").cast(IntegerType))
    .withColumn("minimum_nights", col("minimum_nights").cast(IntegerType))
    .withColumn("maximum_nights", col("maximum_nights").cast(IntegerType))
    .withColumn("minimum_minimum_nights", col("minimum_minimum_nights").cast(IntegerType))
    .withColumn("minimum_nights_avg_ntm", col("minimum_nights_avg_ntm").cast(IntegerType))
    .withColumn("maximum_minimum_nights", col("maximum_minimum_nights").cast(IntegerType))
    .withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType))
    .withColumn("number_of_reviews_ltm", col("number_of_reviews_ltm").cast(IntegerType))
    .withColumn("number_of_reviews_ltm", col("number_of_reviews_ltm").cast(IntegerType))
  val sortedColumns = listingsAirbnb.columns.sorted.map(c=>col(c))
  listingsAirbnb = listingsAirbnb.select(sortedColumns:_*)
  listingsAirbnb = listingsAirbnb.withColumnRenamed("id","listing_id").dropDuplicates("listing_id")
  val dropColumnSeq = Seq("host_name", "host_url", "host_since", "host_location", "host_about", "host_response_time", "host_response_rate", "host_acceptance_rate",
    "host_is_superhost", "host_thumbnail_url", "host_picture_url", "host_neighbourhood", "host_listings_count",
    "host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified")
  (listingsAirbnb.drop(dropColumnSeq:_*), listingsAirbnb)
}


// COMMAND ----------


if (fileIsExists(nameListings)) {
  val (berlinListiningsHost, berlinListinings) = preprocessingListings(pathToFiles+berlinListings, "Berlin")
  berlinListinings.printSchema()
//   display(berlinListinings.printSchema())
}

// COMMAND ----------


