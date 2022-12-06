// Databricks notebook source
// MAGIC %md
// MAGIC # Fetch Data
// MAGIC 
// MAGIC This notebook fetches all files about Airbnb from https://public.opendatasoft.com. These files include all information regarding the booking of apartments in different cities, such as London, Berlin, and Paris

// COMMAND ----------

// MAGIC %sh
// MAGIC pathAirbnb=/dbfs/FileStore/data/airbnb
// MAGIC mkdir -p $pathAirbnb
// MAGIC mkdir -p $pathAirbnb/Berlin
// MAGIC mkdir -p $pathAirbnb/Paris
// MAGIC mkdir -p $pathAirbnb/London
// MAGIC 
// MAGIC wget -O $pathAirbnb/airbnb-listings.csv "https://public.opendatasoft.com/explore/dataset/airbnb-listings/download/?format=csv&timezone=Europe/Helsinki&lang=en&use_labels_for_header=true&csv_separator=%3B"
// MAGIC 
// MAGIC wget -O $pathAirbnb/London/london_listings_2022-09-10.csv.gz "http://data.insideairbnb.com/united-kingdom/england/london/2022-09-10/data/listings.csv.gz"
// MAGIC wget -O $pathAirbnb/London/london_reviews_2022-09-10.csv.gz "http://data.insideairbnb.com/united-kingdom/england/london/2022-09-10/data/reviews.csv.gz"
// MAGIC 
// MAGIC wget -O $pathAirbnb/Paris/paris_listings_2022-09-09.csv.gz "http://data.insideairbnb.com/france/ile-de-france/paris/2022-09-09/data/listings.csv.gz"
// MAGIC wget -O $pathAirbnb/Paris/paris_reviews_2022-09-09.csv.gz "http://data.insideairbnb.com/france/ile-de-france/paris/2022-09-09/data/reviews.csv.gz"
// MAGIC 
// MAGIC wget -O $pathAirbnb/Berlin/berlin_listings_2022-09-15.csv.gz "http://data.insideairbnb.com/germany/be/berlin/2022-09-15/data/listings.csv.gz"
// MAGIC wget -O $pathAirbnb/Berlin/berlin_reviews_2022-09-15.csv.gz "http://data.insideairbnb.com/germany/be/berlin/2022-09-15/data/reviews.csv.gz"
// MAGIC 
// MAGIC find /dbfs/FileStore/data/airbnb/Berlin -name '*.csv.gz' -print0 | xargs -0 -n1 gzip -d
// MAGIC find /dbfs/FileStore/data/airbnb/London -name '*.csv.gz' -print0 | xargs -0 -n1 gzip -d
// MAGIC find /dbfs/FileStore/data/airbnb/Paris -name '*.csv.gz' -print0 | xargs -0 -n1 gzip -d

// COMMAND ----------

// MAGIC %sh 
// MAGIC ls /dbfs/FileStore/data/airbnb

// COMMAND ----------

// MAGIC %sh
// MAGIC mkdir -p /dbfs/FileStore/result/

// COMMAND ----------

// MAGIC %sh
// MAGIC ls /dbfs/FileStore

// COMMAND ----------

// MAGIC %sh
