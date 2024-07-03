import json
import threading
from deals import *
from contacts import *


class Poll:
    def __init__(self):
        pass

    def start(self):
        with open("config.json", "r") as f:
            config = json.load(f)

        threads = []
        deal_instances = []
        contact_instances = []

        """
        ################## DEALS THREADS #################################
        """

        for endpoint in config["deals"]:
            deal_instance = Deals(endpoint, config["deals"])
            deal_instances.append(deal_instance)

        for deal in deal_instances:
            thread = threading.Thread(target=deal.fetch_and_process_data)
            threads.append(thread)

        """
        ################## CONTACTS THREADS #################################
        """
        for endpoint in config["contacts"]:
            contact_instance = Contacts(endpoint, config["contacts"])
            contact_instances.append(contact_instance)

        for contact in contact_instances:
            thread = threading.Thread(target=contact.fetch_and_process_data)
            threads.append(thread)

        for thread in threads:
            print(thread)
            thread.start()

        for thread in threads:
            thread.join()
            print(thread)

        print("\n\n\nAll threads have been processed.")
