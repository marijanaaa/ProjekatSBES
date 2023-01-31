import requests
import findspark
findspark.init()
from pyspark.sql import SparkSession
import numpy as np
import matplotlib.pyplot as plt

# Create a SparkSession
spark = SparkSession.builder.appName("AlienVault").getOrCreate()
# Set your AlienVault API key
api_key = "8efd6a55d157ee2cdaee567709846929faaa8e2620f67f7abf6e34c1db260649"
# Set the base URL for the AlienVault API
base_url = "https://otx.alienvault.com"
# Set the endpoint for the AlienVault API
endpoint = "/api/v1/pulses/subscribed"
# Set the headers for the API request
headers = {
    "X-OTX-API-KEY": api_key
}
# Set the parameters for the API request
params = {
    "limit": 300 # Set the number of pulses to retrieve
}
# Send the API request
response = requests.get(base_url + endpoint, headers=headers, params=params)
# Check the status code of the response
if response.status_code == 200:
    # Convert the response to a JSON object
    import json
    data = response.json()
    
   

    #print(pod)
    # Extract the list of pulses from the response
    pulses = data["results"]
    pod = json.dumps(pulses,default=str)
    with open("Data1.json","w")as outfile:
        outfile.write(pod)


    # Convert the list of pulses to a Spark DataFrame
    df = spark.read.format("json").option("inferSchema", "true").load("Data.json")
    df.createOrReplaceTempView("pulse")#pravim tabelu koja se zove puls
    #spark.sql("select count(indicators) from puls").show()#tri pulsa je ucitalo, treba da ispise da ima 3 indicators
    
    
   # br = 0
    #novi = []
   # dataNew = df.select(df["indicators"].getItem("indicator")).collect()# svi indikatori  jer je 3 pulsa(imamo 3 indicators)(za svaki indicators, njegovi indicator)
    #niz nizova indikatora(u tom nizu ima 3 niza))
    
   # print(indicator[0])
   # for i in dataNew: #prolazi kroz taj veliki niz
       # for x in i:  #x je taj mali niz(mali nizovi u okviru velikog niza)
         # novi.append(x) 

            
    #countries =  spark.sql("select targeted_countries from puls").collect()[0][0]

    #print(countries)
    rows = spark.sql("select targeted_countries from pulse").collect()
    dict = {}
    for row in rows:
        result = row.asDict()
        array = result["targeted_countries"]
        for country in array:
            if country in dict.keys():
                dict[country] += 1
            else:
                dict[country] = 1

    print(dict)
                            
    
    group_data = list(dict.values())
    group_names = list(dict.keys())
    group_mean = np.mean(group_data) 


    fig, ax = plt.subplots()
    ax.barh(group_names, group_data)
    colors = ( "orange", "cyan", "yellow", "grey", "green",)
    plt.title("Most targeted countries")
    plt.savefig("targeted_countries.jpg")


    #MALWARE FAMILIES
    rows = spark.sql("select malware_families from pulse").collect()
    dict = {}
    for row in rows:
        result = row.asDict()
        array = result["malware_families"]
        for country in array:
            if country in dict.keys():
                dict[country] += 1
            else:
                dict[country] = 1

    print(dict)
                            
    
    group_data = list(dict.values())
    group_names = list(dict.keys())
    group_mean = np.mean(group_data) 


    fig, ax = plt.subplots()
    ax.barh(group_names, group_data)
    plt.title("Most common malware families")
    plt.savefig("malware_families.jpg")

    #INDUSTRIES
    rows = spark.sql("select industries from pulse").collect()
    dict = {}
    for row in rows:
        result = row.asDict()
        array = result["industries"]
        for country in array:
            if country in dict.keys():
                dict[country] += 1
            else:
                dict[country] = 1

    print(dict)
                            
    
    group_data = list(dict.values())
    group_names = list(dict.keys())
    group_mean = np.mean(group_data) 


    fig, ax = plt.subplots()
    ax.barh(group_names, group_data)
    plt.title("Most attacked industries")
    plt.savefig("industries.jpg")

    #ADVERSARY
    rows = spark.sql("select adversary from pulse").collect()
    dict = {}
    result = row.asDict()
    for row in rows:
        result = row.asDict()
        print(result)
        value = result["adversary"]
        if value == "":
            continue
        if value in dict.keys():
            dict[value] += 1
        else:
            dict[value] = 1
    print(dict)
                            
    
    group_data = list(dict.values())
    group_names = list(dict.keys())
    group_mean = np.mean(group_data) 


    fig, ax = plt.subplots()
    ax.barh(group_names, group_data)
    plt.title("Adversaries")
    plt.savefig("adversary.jpg")


    #TAGS
    rows = spark.sql("select tags from pulse").collect()
    dict = {}
    for row in rows:
        result = row.asDict()
        array = result["tags"]
        for country in array:
            if country in dict.keys():
                dict[country] += 1
            else:
                dict[country] = 1

    sorted_tuple_list = sorted(dict.items(), key=lambda x:x[1], reverse=True)    
    sorted_tuple_list = sorted_tuple_list[:10]
    print(sorted_tuple_list)

    dict={}
    for tuple in sorted_tuple_list:
        dict[tuple[0]] = tuple[1]

    print("\nDICT\n");
    print(dict)

    group_data = list(dict.values())
    group_names = list(dict.keys())
    group_mean = np.mean(group_data) 


    fig, ax = plt.subplots()
    ax.barh(group_names, group_data)
    plt.title("Tags")
    plt.savefig("tags.jpg")
    plt.show()


    #print(numIndic)           #print(niz)
    #.za svaki indicators ce uzeti njegov prvi indicator
    #dataNEw = spark.sql("select indicators from puls").collect()
   # print(dataNEw)
    #spark.sql("select tlp from puls").show()
    #spark.sql("select indicator from indicators from puls").show()
    #spark.sql("select revision from puls").show
#dataframe.printSchema()
    # Print the DataFrame to the console

else:
    print("Error:", response.status_code)
# Stop the SparkSession
spark.stop()