import requests
import pandas as pd 
import json
import sys
import time

class RequestApi():
    def __init__(self):
        pass
    
    def getAPIResponse(self, endpoint, endpointName):
        response = requests.get(endpoint["api_url"], headers=endpoint["headers"])
        data = response.json()
        if endpoint["schema"].keys() is not None:
            dataFrameData = pd.DataFrame(data,columns=endpoint["schema"].keys())
        else:
            dataFrameData = pd.DataFrame(data)
        dataFrameData.to_csv(endpointName + ".csv", encoding='utf-8', index=False)

def parseArgs():
    args = sys.argv[1:]
    return args

def main():
    # Convert interface file to JSON
    with open('interface.json', 'r') as f:
        interface = json.load(f) 

    test = RequestApi()
    
    endpoint = parseArgs()
    for ep in endpoint:
        print("############################################################")
        print("creating <"+ep+"> dataframe...")
        time.sleep(1)
        test.getAPIResponse(interface[ep], ep)
        time.sleep(1)
        print("<"+ep+"> dataframe finalized")
    print("###########################################################")

if __name__ == "__main__":
    main()
