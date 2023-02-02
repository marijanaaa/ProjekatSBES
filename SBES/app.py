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
    
   
    # Extract the list of pulses from the response
    pulses = data["results"]
    pod = json.dumps(pulses,default=str)
    with open("Data1.json","w")as outfile:
        outfile.write(pod)


    # Convert the list of pulses to a Spark DataFrame
    df = spark.read.format("json").option("inferSchema","true") .load("Data.json")
    df.createOrReplaceTempView("pulse")#pravim tabelu koja se zove puls
            
    def plot(dict, parameter):
        group_data = list(dict.values())
        group_names = list(dict.keys())
        group_mean = np.mean(group_data) 

        fig, ax = plt.subplots()
        ax.barh(group_names, group_data)
        plt.title(parameter)
        plt.savefig(parameter+'.png')


    def display_data(parameter):
        rows = spark.sql('select '+parameter+' from pulse').collect()
        dict = {}
        for row in rows:
            result = row.asDict()
            array = result[parameter]
            for item in array:
                if item in dict.keys():
                    dict[item] += 1
                else:
                    dict[item] = 1
        print(dict)  
        plot(dict, parameter)


    def display_data_adversary():
        rows = spark.sql("select adversary from pulse").collect()
        dict = {}
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
        plot(dict, "adversary")


    def display_data_tags():
        rows = spark.sql("select tags from pulse").collect()
        dict = {}
        for row in rows:
            result = row.asDict()
            array = result["tags"]
            for item in array:
                if item in dict.keys():
                    dict[item] += 1
                else:
                    dict[item] = 1
        sorted_tuple_list = sorted(dict.items(), key=lambda x:x[1], reverse=True)    
        sorted_tuple_list = sorted_tuple_list[:10]
        print(sorted_tuple_list)

        dict={}
        for tuple in sorted_tuple_list:
            dict[tuple[0]] = tuple[1]

        print(dict)
        plot(dict,"tags")

    
    '''def malware_families_in_targeted_country():
        ids = spark.sql("select id from pulse").collect()
        dict = {}
        dict_malwares_frequency = {}
        for id in ids:
            id_as_dict = id.asDict()
            #print(id_as_dict)
            value = id_as_dict.get('id')
            #print(value)
            targeted_countries = spark.sql('select targeted_countries from pulse WHERE id = "'+value+'"').collect()
            malware_families = spark.sql('select malware_families from pulse WHERE id = "'+value+'"').collect()
            for country in targeted_countries:
                country = country.asDict()
                if country in dict.keys():
                    for malware in malware_families:
                        malware = malware.asDict()
                        if malware in dict_malwares_frequency.keys():
                            dict_malwares_frequency[malware] += 1
                            dict[country] = dict_malwares_frequency
                        else:
                            dict_malwares_frequency[malware] = 1
                            dict[country] = dict_malwares_frequency
                else:
                    for malware in malware_families:
                        if malware in dict_malwares_frequency.keys():
                            dict_malwares_frequency[malware] += 1
                            dict[country] = dict_malwares_frequency
                        else:
                            dict_malwares_frequency[malware] = 1
                            dict[country] = dict_malwares_frequency
        print(dict)
                    
    '''

    #TARGETED COUNTRIES
    display_data("targeted_countries")
    
    #MALWARE FAMILIES
    display_data("malware_families")

    #INDUSTRIES
    display_data("industries")

    #ADVERSARY
    display_data_adversary()

    #TAGS
    display_data_tags()
    
    #NUMBER OF MALWARE FAMILIES IN TARGETED COUNTRIES
    #malware_families_in_targeted_country()

else:
    print("Error:", response.status_code)
# Stop the SparkSession
spark.stop()