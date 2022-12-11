// Databricks notebook source
// MAGIC %md
// MAGIC # Processing reviews.csv
// MAGIC 
// MAGIC The file has comments on reviews about the apartment in Airbnb in the city definitions.

// COMMAND ----------

import java.io.File
import org.apache.spark.sql.functions._
import spark.implicits._   

val pathToFile = dbutils.widgets.get("pathToFile")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

var reviewsAirbnb = spark.read.format("csv").option("header", "true").option("sep", ",").load(pathToFile)
reviewsAirbnb = reviewsAirbnb
  .withColumn("city", lit(pathToFile.split("/")(4)))
  .withColumn("listing_id", col("listing_id").cast(IntegerType))
  .withColumnRenamed("id","reviews_id").filter(col("date").isNotNull)
  

display(reviewsAirbnb)


// COMMAND ----------

// MAGIC %md
// MAGIC # Create seporate file with data weather
// MAGIC 
// MAGIC Contribute definite data weather for reviews.csv by comment date. Real temperatures and meteorological code and description of that reviews_id.

// COMMAND ----------


// one arg latitude and longituted
val cityCoordinates: Map[String, List[String]] = Map("London" -> List("51.51", "-0.13"), 
                                                     "Paris" -> List("48.85", "2.35"), 
                                                     "Berlin" -> List("52.52", "13.41"))

val codeWeather = Seq((0, "Clear sky"), (1, "Mainly clear"), (2, "Partly cloudy"), 
                        (3, "Overcast"), (45, "Fog"), (46, "Depositing rime fog"), 
                        (51, "Drizzle: Light"), (53, "Drizzle: Moderate"), (54, "Drizzle: Dense intensity"), 
                        (56, "Freezing Drizzle: Light"), (57, "Freezing Drizzle: Dense intensity"), (61, "Rain: Slight"), 
                        (62, "Rain: Moderate"), (63, "Rain: heavy intensity"), (66, "Freezing Rain: Light"), 
                        (67, "Freezing Rain: Heavy intensity"), (71, "Snow fall: Slight"), (72, "Snow fall: Moderate"), 
                        (73, "Snow fall: Heavy intensity"), (77, "Snow grains"), (80, "Rain showers: Slight"), 
                        (81, "Rain showers: Moderate"), (82, "Rain showers: Violent"), (85, "Snow showers slight"), 
                        (86, "Snow showers heavy"), (95, "Thunderstorm: Slight or moderate"), 
                        (96, "Thunderstorm with slight"), (97, "Thunderstorm with heavy hail"))

val columns = Seq("code", "description")
val weatherCodeDF = spark.createDataFrame(codeWeather).toDF(columns:_*)

display(weatherCodeDF)

// COMMAND ----------


import scala.io.Source
import scala.collection._
import org.json4s.jackson.JsonMethods.parse

def getWeatherOnDate(date: String, city: String):(String, String) = {
  
  implicit val formats = org.json4s.DefaultFormats
  
  val latitude = cityCoordinates(city)(0)
  val longtitude = cityCoordinates(city)(1)
  try {
      val url = s"https://archive-api.open-meteo.com/v1/era5?latitude=${latitude}&longitude=${longtitude}1&start_date=${date}&end_date=${date}&hourly=temperature_2m,weathercode"
  val response = parse(Source.fromURL(url, "utf-8").mkString).extract[Map[String, Any]]
   val hourlyResult: Map[String, List[Double]] = response("hourly").asInstanceOf[Map[String, List[Double]]]
  val temperature = hourlyResult("temperature_2m")(13).toString
  val weatherCode = hourlyResult("weathercode")(13).toString
  (temperature, weatherCode)
  } catch {
    case e: Exception => ("null", "null")
  }
}

// COMMAND ----------

import org.apache.spark.sql.expressions.UserDefinedFunction

val convertUDF: UserDefinedFunction = udf((date: String, city: String) => getWeatherOnDate(date, city))
// val testUdf = udf("udfGetWeatherOnDate", getWeatherOnDate)

var originDateWeather = reviewsAirbnb
  .select("reviews_id", "date", "city")
  .filter(col("date").isNotNull)
  .withColumn("weather", convertUDF(col("date"), col("city")))
  .withColumn("temperature", col("weather._1").cast(StringType))
  .withColumn("weather_code", col("weather._2").cast(StringType))
  .drop(col("weather"))

display(originDateWeather)

// COMMAND ----------

originDateWeather = originDateWeather
  .join(weatherCodeDF, (weatherCodeDF("code") === originDateWeather("weather_code")), "inner")
  .drop(col("code"))
                                   
display(originDateWeather)

// COMMAND ----------

val outputPath = dbutils.widgets.get("outputPath")

originDateWeather.write.options(Map("header"->"true", "escape"->"\"")).csv(outputPath + "weather.csv")  
reviewsAirbnb.write.options(Map("header"->"true", "escape"->"\"")).csv(outputPath + "reviews.csv") 
