import requests
from pyspark.sql import SparkSession
import findspark
findspark.init()
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
    "limit": 100 # Set the number of pulses to retrieve
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
    with open("Data.json","w")as outfile:
        outfile.write(pod)


    # Convert the list of pulses to a Spark DataFrame
    df = spark.read.format("json").option("inferSchema", "true").load("Data.json")
    df.createOrReplaceTempView("puls")#pravim tabelu koja se zove puls
    #spark.sql("select count(indicators) from puls").show()#tri pulsa je ucitalo, treba da ispise da ima 3 indicators
    
    
   # br = 0
    #novi = []
   # dataNew = df.select(df["indicators"].getItem("indicator")).collect()# svi indikatori  jer je 3 pulsa(imamo 3 indicators)(za svaki indicators, njegovi indicator)
    #niz nizova indikatora(u tom nizu ima 3 niza))
    
   # print(indicator[0])
   # for i in dataNew: #prolazi kroz taj veliki niz
       # for x in i:  #x je taj mali niz(mali nizovi u okviru velikog niza)
         # novi.append(x) 

            
    countries =  spark.sql("select targeted_countries from puls").collect()[0][0]

    #print(countries)
    countries1 = spark.sql("select targeted_countries from puls").collect()
    my_dictionary = dict.fromkeys(countries)

    
    

   
    for i in countries1:
        for x in i:
            for n in x:
               if n in my_dictionary.keys():
                   if my_dictionary[n] == None:
                        my_dictionary[n] = 1
                   else:
                     my_dictionary[n] = my_dictionary[n] + 1
                   
    print(my_dictionary)           
    
    group_data = list(my_dictionary.values())
    group_names = list(my_dictionary.keys())
    group_mean = np.mean(group_data) 



    fig, ax = plt.subplots()
    ax.barh(group_names, group_data)
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