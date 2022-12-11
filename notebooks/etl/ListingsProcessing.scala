// Databricks notebook source
// MAGIC %md
// MAGIC # Processing listings.csv
// MAGIC 
// MAGIC The file contains all the information concerning the AirBnb apartments.

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._   
import org.apache.spark.sql.DataFrame


val pathToFile = "/FileStore/data/airbnb/London/london_listings_2022-09-10.csv"


// COMMAND ----------

import org.apache.spark.sql.types.IntegerType
var listiningsAirbnb = spark.read.format("csv").option("header", "true").option("sep", ",").load(pathToFile)

listiningsAirbnb = listiningsAirbnb
  .withColumn("id", col("id").cast(IntegerType))
  .withColumn("host_id", col("host_id").cast(IntegerType))
  .withColumn("bedrooms", col("bedrooms").cast(IntegerType))
  .withColumn("beds", col("beds").cast(IntegerType))
  .withColumn("minimum_nights", col("minimum_nights").cast(IntegerType))
  .withColumn("maximum_nights", col("maximum_nights").cast(IntegerType))
  .withColumn("availability_30", col("availability_30").cast(IntegerType))
  .withColumn("availability_60", col("availability_90").cast(IntegerType))
  .withColumn("availability_90", col("availability_30").cast(IntegerType))
  .withColumn("availability_365", col("availability_30").cast(IntegerType))
  .withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType))
  .withColumn("number_of_reviews_ltm", col("number_of_reviews_ltm").cast(IntegerType))
  .withColumn("number_of_reviews_ltm", col("number_of_reviews_ltm").cast(IntegerType))
  .withColumn("calculated_host_listings_count_private_rooms", col("calculated_host_listings_count_private_rooms").cast(IntegerType))
  .withColumn("calculated_host_listings_count_entire_homes", col("calculated_host_listings_count_entire_homes").cast(IntegerType))
  .withColumn("calculated_host_listings_count_shared_rooms", col("calculated_host_listings_count_shared_rooms").cast(IntegerType))
  .withColumn("calculated_host_listings_count", col("calculated_host_listings_count").cast(IntegerType))
  
display(originValues)


// COMMAND ----------

val sortedColumns = listiningsAirbnb.columns.sorted.map(c=>col(c))
listiningsAirbnb = listiningsAirbnb.select(sortedColumns:_*).withColumn("city", lit(pathToFile.split("/")(4)))
listiningsAirbnb = listiningsAirbnb.withColumnRenamed("id","listing_id").dropDuplicates("listing_id")

display(listiningsAirbnb)

// COMMAND ----------

val listiningsWithoutNull = listiningsAirbnb.filter(col("listing_id").isNotNull)

display(listiningsWithoutNull)

// COMMAND ----------

val listiningsHost = listiningsWithoutNull.select("host_id", "host_name", "host_url", "host_since", "host_location", "host_about", "host_response_time", "host_response_rate", "host_acceptance_rate", "host_is_superhost", "host_thumbnail_url", "host_picture_url", "host_neighbourhood", "host_listings_count",
    "host_total_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified", "city")

display(listiningsHost)

// COMMAND ----------

val outputPath = dbutils.widgets.get("outputPath")

listiningsHost.write.options(Map("header"->"true", "escape"->"\"")).csv(outputPath + "host.csv")  
listiningsWithoutNull.write.options(Map("header"->"true", "escape"->"\"")).csv(outputPath + "listings.csv") 
