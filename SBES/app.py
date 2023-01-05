import sys
import findspark
findspark.init() 
from OTXv2 import OTXv2, IndicatorTypes
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, BooleanType, ArrayType
import requests, json
import argparse
import is_malicious


API_KEY = "8efd6a55d157ee2cdaee567709846929faaa8e2620f67f7abf6e34c1db260649"
OTX_SERVER = 'https://otx.alienvault.com/'
otx = OTXv2(API_KEY, server=OTX_SERVER)
#sesija za spark
spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()#app name is just session name

#odavde uzimam jedan otx objekat, tj virus
data_from_otx = otx.getall_iter(max_page=5)
#upisujem ga u json fajl
for i in data_from_otx:
    json_object = json.dumps(i,default=str)
    with open("CTI_Data.json","w")as outfile:
        outfile.write(json_object)

#df2 = spark.read.json("data_from_otx.json")
#ispise se schema koja je napravljena u sparku od json obj koji sam ucitala
#sa alien vaulta a taj obj predstavlja neki vid obj koji opisuje virus
#df2.printSchema()
#da mi iz liste indikatora ispise polje indikator prvog iz liste
#df2.select(df2["indicators"].getItem("indicator")[0]).show()

dataframe = spark.read.json("CTI_Data.json")
#dataframe.printSchema()
#dataframe.select(dataframe["Source"]).show()

parser = argparse.ArgumentParser(description='OTX CLI Example')
parser.add_argument('-ip', help='IP eg; 4.4.4.4', required=False)
parser.add_argument('-host',
                    help='Hostname eg; www.alienvault.com', required=False)
parser.add_argument(
    '-url', help='URL eg; http://www.alienvault.com', required=False)
parser.add_argument(
    '-hash', help='Hash of a file eg; 7b42b35832855ab4ff37ae9b8fa9e571', required=False)
parser.add_argument(
    '-file', help='Path to a file, eg; malware.exe', required=False)

#private ip adrese
'''Class A: 10.0.0.0 to 10.255.255.255
   Class B: 172.16.0.0 to 172.31.255.255
   Class C: 192.168.0.0 to 192.168.255.255'''
'''def func(ip):
    print("------------------")
    print(ip)
    splited_ip = ip.split('.')
    if splited_ip[0] == '10' and int(splited_ip[1]) <= 255 or splited_ip[0] == '172' and int(splited_ip[1]) <= 31 or splited_ip[0] == '192' \
        and int(splited_ip[1]) <= 168 or splited_ip[0] == '0':
        print("Private ip")
        print("------------------")
    else:
        args = vars(parser.parse_args(("-ip "+ip).split()))
        is_malicious.is_malicious(otx, args)

for iterator in dataframe.collect():
    func(iterator["Source"])'''

if __name__ == "__main__":
    print(f"HEllo, I'm Python VErsion : {sys.version}")