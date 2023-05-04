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
