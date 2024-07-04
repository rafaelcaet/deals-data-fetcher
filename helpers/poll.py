import pandas as pd
import json
import threading
from dealFetchData.deals import *
from dealFetchData.dealContact import *
from dealFetchData.dealCustomFieldData import *


class Poll:

    def __init__(self):
        pass

    def start(self):

        ################# Read files ######################################

        with open("config.json", "r") as f:
            config = json.load(f)

        ############## List and variables ################################

        threads = []
        results = []
        lock = threading.Lock()
        deal_instances = []
        ################## Deals Threads #################################

        for endpoint in config["deals"]:
            deal_instance = Deals(endpoint, config["deals"])
            deal_instances.append(deal_instance)

        for deal in deal_instances:
            thread = threading.Thread(target=deal.fetch_and_process_data)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        ################## DealCustomFieldData Threads ###################
        print(f">  Start Threads to fetch dealCustomFieldData \n")

        df_deals = pd.read_csv("./others/deals.csv")

        for deal_custom_field_data_link in df_deals[
                'links.dealCustomFieldData']:
            thread = threading.Thread(target=fetch_custom_field_data,
                                      args=(deal_custom_field_data_link,
                                            config["customField"], results,
                                            lock))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        final_df = pd.concat(results, ignore_index=True)
        final_df.to_csv("./others/dealCustomFieldData.csv",
                        encoding="utf-8-sig",
                        index=False)

        print("\n\n\nAll threads have been processed.")
