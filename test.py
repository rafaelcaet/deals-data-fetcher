import requests
import pandas as pd 
import json
import os
import time
 
# Clearing the Screen
def clearScreen():
    os.system('clear')

class RequestApi():
    # Constructor
    def __init__(self):
        pass
    
    # Get the  Active API response
    # @params endpoint :  Object with api_url and headers
    # @params endpointName: String
    def getAPIResponse(self, interfaces:json):
        print("> Start script \n")  
######################################################################################################################
#                   Deals dataframe
####################################################################################################################
        time.sleep(1)
        print("####### Creating deals dataframes ########\n")
        try:  
            response = requests.get(interfaces['data']['api_url'], headers=interfaces["data"]["headers"],params=interfaces["data"]["params"])
            data = response.json()  
            # Normalizing json data from dealsCustomFieldData
            normalizedData = pd.json_normalize(data['dealCustomFieldData'])
            
            # Verify interface schema
            if len(interfaces["data"]["schema"]) != 0:
                dfData = pd.DataFrame(normalizedData,columns=interfaces["data"]["schema"].keys())
            else:
                dfData = pd.DataFrame(normalizedData)
            print('> dealsCustomFieldData dataframe was created!\n')
        except ValueError as error:
                print(error)
        try:
            response = requests.get(interfaces['meta']['api_url'], headers=interfaces["meta"]["headers"])
            data = response.json()           
            # Normalizing json data from dealsCustomFieldMeta
            normalizedData = pd.json_normalize(data['dealCustomFieldMeta'])
                
            # Verify interface schema            
            if len(interfaces["meta"]["schema"]) != 0:
                dfMeta = pd.DataFrame(normalizedData,columns=interfaces["meta"]["schema"].keys())
            else:
                dfMeta = pd.DataFrame(normalizedData)
            print('> dealsCustomFieldMeta dataframe was created!\n')
        except ValueError as error:
                print(error)        
        try:     
            # type casting to string 
            dfMeta['id'] = dfMeta['id'].astype(str)
            dfData['customFieldId'] = dfData['customFieldId'].astype(str)

            # merging dataFrames
            df_merged = dfData.merge(dfMeta, left_on='customFieldId', right_on='id', how='left', suffixes=('_dfData', '_dfMeta'))
            
            print('> df_merged dataframe was created!\n')

            # pivoting dataFrames
            df_pivoted = df_merged.pivot(columns='fieldLabel', values='fieldValue')       
            df_pivoted = df_pivoted.reset_index()
            df_pivoted.columns.name = None
            
            print('> df_pivoted dataframe was created!\n')
            
            # Export all dataFrames to csv
            print('> Exported all dataframes to csv!')
            dfMeta.to_csv("csv_dealsCustomFieldMeta.csv", encoding='utf-8-sig', index=False)
            dfData.to_csv("csv_dealsCustomFieldData.csv", encoding='utf-8-sig', index=False)
            df_merged.to_csv('csv_merged.csv', encoding='utf-8-sig', index=False)
            df_pivoted.to_csv('csv_pivoted.csv', encoding='utf-8-sig', index=False)
            df_pivoted.to_excel('csv_pivoted.xlsx', index=False, engine='openpyxl')
            print(df_pivoted)
        except ValueError or KeyError as error:
            print(error)

def main():
    
    # Convert interface file to JSON
    with open('interfaces.json', 'r') as f:
        interfaces = json.load(f)
         
    # New RequestApi Instance
    test = RequestApi()
    
  # Calling getApiResponse to build all dataframes
    clearScreen()
    test.getAPIResponse(interfaces)


if __name__ == "__main__":
    main()
