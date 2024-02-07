# Databricks notebook source
# DBTITLE 1,Loading the dataset into a PySpark DataFrame
#reading the given csv file into a dataframe and inferring the schema based on the data in the columns

df = spark.read.format('csv').options(header = True, inferSchema = True).load('dbfs:/FileStore/database.csv')
display(df)

# COMMAND ----------

# DBTITLE 1,Converting Date column into a timestamp column
#importing the required functions

from pyspark.sql.functions import expr, to_date, to_timestamp, date_format

# Time column after inferring the schema has timestamp type. 
# Below query converts Date column into timestamp column and the result will be stored in a new dataframe df1

df1 = df.withColumn('Date', expr("case when Date like '____-%' then cast(Date as timestamp) else cast(date_format(to_date(Date, 'MM/dd/yyyy'), 'yyyy-MM-dd') as timestamp) end "))
display(df1)

# COMMAND ----------

# DBTITLE 1,Filtering the dataset to include only earthquakes with a magnitude greater than 5.0
#using the dataframe filter function to fetch the required data into another dataframe df2 and displaying it

df2 = df1.filter(df1.Type == 'Earthquake').filter(df1.Magnitude > 5.0)
display(df2)

# COMMAND ----------

# DBTITLE 1,Calculating the average depth and magnitude of earthquakes for each earthquake type.
#importing the required functions to measure average value

from pyspark.sql.functions import avg, when

# Using groupBy logic to fetch the average values based on the Earthquake Type

df3 = df1.groupBy('Type').agg(avg('Depth').alias('Average_Depth'), avg('Magnitude').alias('Average_Magnitude'))
display(df3)

# COMMAND ----------

# DBTITLE 1,Implementing a UDF to categorize the earthquakes into levels based on their magnitudes
# creating an UDF that takes an dataframe as input and returns an dataframe that contains an extra column that contains categories based on magnitudes

def categorize_types(df):
    df1 = df.withColumn('Earthquake_Category', when( (df.Magnitude > 0) & (df.Magnitude <= 5.5), 'Low').when((df.Magnitude > 5.5) & (df.Magnitude <= 6), 'Moderate').when((df.Magnitude > 6), 'High'))
    return df1    

# COMMAND ----------

# calling the function created above and displaying the result

res_df = categorize_types(df1)
display(res_df)

# COMMAND ----------

# DBTITLE 1,Calculating the distance of each earthquake from a reference location
#Importing the sqrt function to measure the distance between to locations

from pyspark.sql.functions import sqrt

# Assuming the reference location as (0, 0) and calculating the distance in a new column named Distance

df4 = df1.withColumn('Distance', sqrt((df.Longitude - 0)**2 + (df.Latitude - 0)**2) )
display(df4)

# COMMAND ----------

# DBTITLE 1,Visualizing the geographical distribution of earthquakes on a world map
# Visualizing the source dataframe loaded using the csv file, based on the location codes
# This map is an inbuilt feature from databricks that takes location codes and the magnitude parameter the display the intensity

display(df)
