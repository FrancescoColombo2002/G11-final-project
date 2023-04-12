# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------


"""
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")
"""

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OKkkkkkkkkkkkkkkkkkkkkkkk"}))
# comment

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/raw/weather/"))

# COMMAND ----------

# Read the CSV file into a DataFrame
df_weather = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv")

# Display the DataFrame
display(df_weather)

# COMMAND ----------


# Print the schema of the DataFrame
df_weather.printSchema()

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/raw/bike_trips/"))

# COMMAND ----------

df_bikeTrips = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/raw/bike_trips/202211_citibike_tripdata.csv")

# Display the DataFrame
display(df_bikeTrips)

# COMMAND ----------

filtered_df = df_bikeTrips.filter(df_bikeTrips.start_station_name == "Cleveland Pl & Spring St")
