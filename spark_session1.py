# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 13:00:30 2024

@author: vijay
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as func

#creating Spark Session
spark = SparkSession.builder.appName("FirstApp").getOrCreate()
print("spark session build")

#Definig schema for your Dataframe
mySchema = StructType([\
    StructField("UserID",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("friends",IntegerType(),True),   
])
    
    
#Creating Dataframe on a csv file
people = spark.read.format("csv")\
    .schema(mySchema)\
    .option("path","C:/Users/vijay/.jupyter/fakefriends.csv")\
    .load()

#performing all transformations
output = people.select(people.UserID,people.name,people.age,people.friends)\
    .where(people.age<30).withColumn('insert_ts',func.current_timestamp())\
    .orderBy(people.UserID)

#taking the count of o/p dataframe
output.count()
#output.show()

#Creating a Temp View
output.createOrReplaceTempView("peoples")

result = spark.sql("select name from peoples")
result.show()