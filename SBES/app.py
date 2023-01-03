import sys
import findspark
findspark.init() 
from OTXv2 import OTXv2, IndicatorTypes
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, BooleanType, ArrayType
import requests, json
import argparse

API_KEY = "8efd6a55d157ee2cdaee567709846929faaa8e2620f67f7abf6e34c1db260649"
otx = OTXv2(API_KEY)
spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()#app name is just session name

data_from_otx = otx.getall_iter(max_page=1)
for i in data_from_otx:
    json_object = json.dumps(i,default=str)
    with open("data_from_otx.json","w")as outfile:
        outfile.write(json_object)

#columns = ["id","indicator","type","created", "content","title","description","expiration","is_active","role"]
#dataframe = spark.read.json("data_from_otx.json")
#data_frame = spark.createDataFrame(data)
#list = dataframe.select("indicators")

schema = StructType([ \
    StructField("id", StringType(), True), \
    StructField("name", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("author_name", StringType(), True), \
    StructField("modified", StringType(), True), \
    StructField("created", StringType(), True), \
    StructField("revision", IntegerType(), True), \
    StructField("tlp", StringType(), True), \
    StructField("public", IntegerType(), True), \
    StructField("adversary", StringType(), True), \
    StructField('indicators', ArrayType(
        StructType([ \
            StructField("id", IntegerType(), True), \
            StructField("indicator", StringType(), True), \
            StructField("type", StringType(), True), \
            StructField("created", StringType(), True), \
            StructField("content", StringType(), True), \
            StructField("title", StringType(), True), \
            StructField("description", StringType(), True), \
            StructField("expiration", StringType(), True), \
            StructField("is_active", IntegerType(), True), \
            StructField("role", StringType(), True) \
            ]) \
        )), \
        
    StructField("tags",ArrayType(StringType()),True), \
    StructField("targeted_countries",ArrayType(StringType()),True), \
    StructField("malware_families", ArrayType(StringType()), True), \
    StructField("attack_ids", ArrayType(StringType()), True), \
    StructField("referencies", ArrayType(StringType()), True), \
    StructField("industries", ArrayType(StringType()), True), \
    StructField("extract_source", ArrayType(StringType()), True), \
    StructField("more_indicators", BooleanType(), True) 
  ])

df2 = spark.read.schema(schema).json("data_from_otx.json")
df2.select("indicators").show()

if __name__ == "__main__":
    print(f"HEllo, I'm Python VErsion : {sys.version}")