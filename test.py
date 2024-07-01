import requests
import pandas as pd 
import json
import sys


class RequestApi():
    def __init__(self):
        pass
    
    def getAPIResponse(self,endpoint,endpointName):
        response =  requests.get(endpoint["api_url"], headers=endpoint["headers"])
        # print(response.json())
        dataFrame = pd.DataFrame(response.json())
        dataFrame.to_csv(endpointName+".csv", encoding='utf-8', index=False)
        print(dataFrame)  
           

def parseArgs():
    args = sys.argv[1:]
    return args

def main():
    
    #convert interface file to JSON
    with open('interface.json', 'r') as f:
        interface = json.load(f) 

    test = RequestApi()
    
    endpoint = parseArgs().pop()
    print("######## creating <"+endpoint+"> dataframe...################")
    test.getAPIResponse(interface[endpoint],endpoint)
    print("############################################################")



if __name__ == "__main__":
    main()