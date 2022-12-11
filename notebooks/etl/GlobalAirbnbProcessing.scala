// Databricks notebook source
// MAGIC %md
// MAGIC # Processing global Airbnb listings.csv
// MAGIC 
// MAGIC The file from https://public.opendatasoft.com/explore/dataset/airbnb-listings/table/?disjunctive.host_verifications&disjunctive.amenities&disjunctive.features&refine.host_response_rate=50 about different cities and all information about apartments.

// COMMAND ----------

import java.io.File
import org.apache.spark.sql.functions._
import spark.implicits._   

// COMMAND ----------

// /FileStore/data/airbnb/airbnb-listings.csv
// val pathToFile = dbutils.widgets.get("pathToFile")

val nameColumnToDrop = Seq("Listing Url", "Scrape ID", "Space", "Description", "Notes", "Transit", "Access", "House Rules", "Summary", "Integration", "Thumbnail Url", "Medium Url", "Picture Url", "XL Picture Url", "Market", "Smart Location", "State", "Zipcode", "Country", "Bed Type", "Weekly Price", "Monthly Price", "Square Feet", "Security Deposit", "Cleaning Fee", "Guests Included", "Extra People", "Jurisdiction Names", "Cancellation Policy", "Features", "Geolocation", "Street", "Country Code")


var globalAirbnb = spark.read.format("csv").option("header", "true").option("sep", ";").load("/FileStore/data/airbnb/airbnb-listings.csv")

globalAirbnb = globalAirbnb.drop(nameColumnToDrop:_*)

val columnsLowerCase = globalAirbnb.columns.map(_.toLowerCase).map(_.replace(" ","_"))

globalAirbnb  = globalAirbnb
  .toDF(columnsLowerCase:_*)

globalAirbnb = globalAirbnb
  .withColumn("scraped_year", year(globalAirbnb("last_scraped")))
  
display(globalAirbnb)

// COMMAND ----------

val nameCitites = dbutils.widgets.get("cities")
val listCities = nameCitites.split(",")

val filteringGlobalDF = globalAirbnb.filter(col("city").isin(listCities: _*))
display(filteringGlobalDF)

// COMMAND ----------

val outputPath = dbutils.widgets.get("outputPath")

filteringGlobalDF.write.options(Map("header"->"true", "escape"->"\"")).csv(outputPath + "global_listings.csv")  

// COMMAND ----------



// COMMAND ----------


