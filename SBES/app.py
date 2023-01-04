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
#sesija za spark
spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()#app name is just session name

#odavde uzimam jedan otx objekat, tj virus
data_from_otx = otx.getall_iter(max_page=1)
#upisujem ga u json fajl
for i in data_from_otx:
    json_object = json.dumps(i,default=str)
    with open("data_from_otx.json","w")as outfile:
        outfile.write(json_object)

df2 = spark.read.json("data_from_otx.json")
#ispise se schema koja je napravljena u sparku od json obj koji sam ucitala
#sa alien vaulta a taj obj predstavlja neki vid obj koji opisuje virus
df2.printSchema()
#da mi iz liste indikatora ispise polje indikator prvog iz liste
df2.select(df2["indicators"].getItem("indicator")[0]).show()

if __name__ == "__main__":
    print(f"HEllo, I'm Python VErsion : {sys.version}")