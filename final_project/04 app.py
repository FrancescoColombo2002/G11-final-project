# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

print("YOUR CODE HERE...")

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------

# MAGIC %pip install folium

# COMMAND ----------

import folium

# Create a map centered at the given latitude and longitude
lat, lon = 40.722062, -73.997278
map = folium.Map(location=[lat, lon], zoom_start=12)

# Add a marker at the given latitude and longitude
folium.Marker(location=[lat, lon], icon=folium.Icon(color='blue')).add_to(map)

# Display the map
map

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType

# Create a list of tuples with string and integer values
data = [('12:00:00', 35), ('2023-05-04 13:00:00', 30), ('2023-05-04 14:00:00', 45),('2023-05-04 15:00:00', 50), ('2023-05-04 16:00:00', 40),('2023-05-04 17:00:00', 35), ('2023-05-04 18:00:00', 40),('2023-05-04 19:00:00', 50), ('2023-05-04 20:00:00', 40)]

# Create a PySpark dataframe from the list of tuples
df = spark.createDataFrame(data, ['time', 'value'])

# Convert the 'time' column to timestamp
df = df.withColumn('time', to_timestamp('time'))

# Show the resulting dataframe
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, date_format

# Assume df is your PySpark dataframe with a datetime column 'datetime_col'
df = df.select(date_format(col('time'), 'HH:mm:ss').alias('time'), 'value')

# COMMAND ----------

df_with_time_only.show()

# COMMAND ----------

import matplotlib.pyplot as plt

time_values = [row.time for row in df.select('time').collect()]
value_values = [row.value for row in df.select('value').collect()]

# COMMAND ----------

plt.plot(time_values, value_values)
plt.ylim([0, max(value_values)+30])
plt.axhline(y=40, color='r', linestyle='--')
plt.show()
