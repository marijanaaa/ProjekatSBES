import requests
import findspark
findspark.init()
from pyspark.sql import SparkSession
import numpy as np

import matplotlib.pyplot as plt
import pandas as pd

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
    "limit": 500 # Set the number of pulses to retrieve
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

    def pandas(dict, parameter):
        group_data = list(dict.values())
        group_names = list(dict.keys())
       
        plt.pie(group_data, labels=group_names)
        plt.title(parameter)
        plt.savefig(parameter+'.png')
 
    def barr(dict, parameter):
        group_data = list(dict.values())
        group_names = list(dict.keys())
        group_mean = np.mean(group_data) 

        fig, ax = plt.subplots()
        ax.barh(group_names, group_data)
        plt.title(parameter)
        group_data = list(dict.values())
        group_names = list(dict.keys())
       
       
        plt.title(parameter)
       
        plt.savefig(parameter+'.png', facecolor='y', bbox_inches="tight",pad_inches=0.3, transparent=True)
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
    def display_malware_families():
        rows = spark.sql('select malware_families from pulse').collect()
        dict = {}
        for row in rows:
            result = row.asDict()
            array = result['malware_families']
            for item in array:
                if item in dict.keys():
                    dict[item] += 1
                else:
                    dict[item] = 1
        print(dict)
        sorted_tuple_list = sorted(dict.items(), key=lambda x:x[1], reverse=True)    
        sorted_tuple_list = sorted_tuple_list[:10]
        
        dict1={}
        for tuple in sorted_tuple_list:
            dict1[tuple[0]] = tuple[1]
        
        plt.clf()
   
        plot(dict1, 'malware_families')


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
        barr(dict, "adversary")


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
        pandas(dict,"tags")

    def threatNumOverTime():
        indicatorss = spark.sql("select indicators from pulse").collect()
        my_dictionary = dict()
        my_dictionary1 = dict()
        created = []
        created1 = []
        for indicators in indicatorss:
            for indicator in indicators:
                for ind in indicator:
                    nind = str(ind[1])
                    splitValue = nind.split("T")[0]

                    if '2023' in splitValue:
                       newValue = splitValue.split('2023-')[1] 
                       val1 = newValue.split('01-')[1] 
                       
                       if val1 <= '20':
                          created.append(newValue)

                    else:
                        newWalue = splitValue.split('2022-')[1]
                        created1.append(newValue)

        my_dictionary = dict.fromkeys(created, 0)
        my_dictionary1 = dict.fromkeys(created1, 0)
        for i in created:
             if i in my_dictionary.keys():
                my_dictionary[i] += 1
             else:
                 my_dictionary[i] = 0
        for i in created1:
             if i in my_dictionary1.keys():
                my_dictionary1[i] += 1
             else:
                 my_dictionary1[i] = 0

        x = my_dictionary.keys()
        y = []
        for i in x:
            y.append(my_dictionary[i])

        x1 = my_dictionary1.keys()
        y1= []
        for i in x1:
            y1.append(my_dictionary1[i])      
        
        list1= sorted(x, reverse = False)
        list2 = sorted ( x1, reverse = False)
        print(list1)
        print(list2)
        plt.clf()

        fig, (ax1, ax2) = plt.subplots(1, 2)
        

        ax1.bar(list1, y, label='2023 year', color = "black")
        ax2.bar(list2, y1, label='2022 year', color='red')
        ax1.set_ylabel("Indicators")
        ax2.set_ylabel("Indicators")
        ax2.set_title('2022 year')
        ax1.set_title('2023 year')
        plt.savefig('Time.png')
        


    

    def malware_families_in_targeted_country():
        ids = spark.sql("select id from pulse").collect()
        country_dict = {}

        for id in ids:
            id = id.asDict()
            id_value = id.get('id')
            targeted_countries = spark.sql('select targeted_countries from pulse WHERE id = "'+id_value+'"').collect() 
            malware_families = spark.sql('select malware_families from pulse WHERE id = "'+id_value+'"').collect()
            temp_country_dict = {}
            temp_malware_dict = {}

            for row_list in targeted_countries:
                for list in row_list:
                    for country in list:
                        if country not in country_dict.keys() and country not in temp_country_dict.keys():
                            country_dict.update({country: {}})
                        temp_country_dict.update({country: {}})

            for row_list in malware_families:
                for list in row_list:
                    for malware in list:
                        if malware not in temp_malware_dict.keys():
                            temp_malware_dict.update({malware:1})
                        else:
                            temp_malware_dict[malware] +=1
            malware_dict = {}
            for country in temp_country_dict.keys():
                malware_dict = country_dict.get(country)
                for malware in malware_dict:
                    if malware in temp_malware_dict:
                        temp_malware_dict[malware] += malware_dict[malware]
                new_dict = dict(malware_dict, **temp_malware_dict)
                country_dict.update({country:new_dict})

        x1 = []
        y1 = []
        
        country1 = 'Canada'
        list_malware1 = country_dict[country1]
        x1 = list_malware1.keys()
        for i in x1:
            y1.append(list_malware1[i])
        
        x2 = []
        y2 = []
        country2 = 'France'
        list_malware2 = country_dict[country2]
        x2 = list_malware2.keys()
        for i in x2:
            y2.append(list_malware2[i])

        x3 = []
        y3 = []
        country3 = 'Russian Federation'
        list_malware3 = country_dict[country3]
        x3 = list_malware3.keys()
        for i in x3:
            y3.append(list_malware3[i])

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3)
        

        ax1.bar(x1, y1, label='Canada', color = "red")
        ax2.bar(x2, y2, label='France', color='blue')
        ax3.bar(x3, y3, label='Russian', color='purple')
        
        ax1.set_title('Canada')
        ax2.set_title('France')
        ax3.set_title('Russian Federation')
        
        plt.savefig('malwares_in_country.png')    
            
        print(country_dict)                   
    
    display_data_tags()

    #TARGETED COUNTRIES
    display_data("targeted_countries")
    
    #MALWARE FAMILIES
    display_malware_families()

    #INDUSTRIES
    display_data("industries")
    
    display_data_adversary()
    
    threatNumOverTime()
    malware_families_in_targeted_country()

   

else:
    print("Error:", response.status_code)
# Stop the SparkSession
spark.stop()