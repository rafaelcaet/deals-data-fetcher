import pandas as pd
import time
import json
import os
import threading
from dealFetchData.deals import *
from dealFetchData.dealOwner import *
from dealFetchData.dealCustomFieldData import *
from dealFetchData.dealContact import *
from datetime import datetime


class Poll:

    def __init__(self):
        pass

    def start(self):
        try:
            date_now = datetime.now()
            print(
                f"************************************************************\n\t\t>>> Initializing Poll <<<\n\n* Poll started at {date_now.strftime('%d/%m/%Y %H:%M')}\n************************************************************"
            )
            ################# Read files and logger ##########################

            if not os.path.exists('logs'): os.makedirs('logs')
            with open('./logs/logger.txt', 'a') as f:
                f.write(
                    f"Poll started at {date_now.strftime('%d/%m/%Y %H:%M')}\n")

            with open("config.json", "r") as f:
                config = json.load(f)

            ############## List and variables ################################

            threads = []
            lock = threading.Lock()
            deal_instances = []
            final_df = None

            start_time = time.time()
            ################## Deals Threads #################################
            print(f">> started a fetch thread to deals")
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
            print(f">> started a fetch thread to dealCustomFieldData")
            results = []
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
            final_df = None
            ################## DealOwner Threads ###########################
            print(f">> started a fetch thread to dealOnwer")
            results = []

            for deal_owner_link in df_deals['links.owner']:
                thread = threading.Thread(target=fetch_deal_owner,
                                          args=(deal_owner_link,
                                                config["dealOwner"], results,
                                                lock))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            final_df = pd.concat(results, ignore_index=True)
            final_df.to_csv("./others/dealsOwner.csv",
                            encoding="utf-8-sig",
                            index=False)
            final_df = None
            ################## DealContact Threads ########################
            print(f">> started a fetch thread to dealOnwer")
            results = []

            for deal_contact_link in df_deals['links.contact']:
                thread = threading.Thread(target=fetch_deal_contact,
                                          args=(deal_contact_link,
                                                config["dealContact"], results,
                                                lock))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            final_df = pd.concat(results, ignore_index=True)
            final_df.to_csv("./others/dealContact.csv",
                            encoding="utf-8-sig",
                            index=False)

            end_time = time.time()
            total_time = end_time - start_time
            print(
                f"\n\nAll threads have been processed in {total_time:,.2f} seconds.\n\n"
            )
        except RuntimeError as e:
            with open('./logs/logger.txt', 'a') as f:
                f.write(
                    f"Poll stopped at {date_now.strftime('%d/%m/%Y %H:%M')}\n>[ERROR] Runtime error {e}"
                )
