// Databricks notebook source
// MAGIC %md
// MAGIC # Global Airbnb ETL
// MAGIC ....

// COMMAND ----------

import java.io.File
import org.apache.spark.sql.functions._
import spark.implicits._   

val pathToFiles = "/FileStore/data/airbnb/"
val airbnbListinings = "airbnb-listings.csv"
val pathOutput = "/FileStore/result/global"

// COMMAND ----------

val nameColumnToDrop = Seq("Listing Url", "Scrape ID", "Space", "Description", "Notes", "Transit", "Access", "House Rules", "Summary", "Integration", "Thumbnail Url", "Medium Url", "Picture Url", "XL Picture Url", "Market", "Smart Location", "State", "Zipcode", "Country", "Bed Type", "Weekly Price", "Monthly Price", "Square Feet", "Security Deposit", "Cleaning Fee", "Guests Included", "Extra People", "Jurisdiction Names", "Cancellation Policy", "Features", "Geolocation")


var globalAirbnb = spark.read.format("csv").option("header", "true").option("sep", ";").load(pathToFiles+airbnbListinings)
globalAirbnb  = globalAirbnb
  .drop(nameColumnToDrop:_*)
  .withColumn("Scraped Year", year(globalAirbnb("Last Scraped")))

display(globalAirbnb)

// COMMAND ----------

val londonGlobal = globalAirbnb.filter(col("city") === "London")

londonGlobal.write.partitionBy("Scraped Year").parquet(pathOutput + "/London") 

display(londonGlobal)

// COMMAND ----------

val berlinGlobal = globalAirbnb.filter(col("city") === "Berlin")

londonGlobal.write.partitionBy("Scraped Year").parquet(pathOutput + "/Berlin")

display(berlinGlobal)

// COMMAND ----------

val parisGlobal = globalAirbnb.filter(col("city") === "Paris")

londonGlobal.write.partitionBy("Scraped Year").parquet(pathOutput + "/Paris")

display(parisGlobal)

// COMMAND ----------

// MAGIC %md
// MAGIC ...

// COMMAND ----------


