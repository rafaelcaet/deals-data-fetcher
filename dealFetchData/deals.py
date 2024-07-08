import requests
import pandas as pd
from datetime import datetime
import time
import os


class Deals:

    # Constructor
    def __init__(self, config_type, config):
        self.config_type = config_type
        self.config = config

    def fetch_and_process_data(self):
        """
        Fetch data from API using config.json file and handle pagination
        @Params
            config_type: string,
            config: dict
        """

        # Get date
        date_now = datetime.now()

        # Format date
        formatted_date = date_now.strftime("%Y-%m-%d")
        date_filter = "?filters[updated_after]=" + formatted_date

        time.sleep(1)

        # lists and variables
        df_list = []
        df_list_data = []
        current_offset = 0
        limit = int(self.config[self.config_type]["params"]["limit"])

        try:
            print(f"> Started a fetch to -> {self.config_type}")
            while True:

                # Request to the API
                response = requests.get(
                    self.config[self.config_type]["api_url"] + date_filter,
                    headers=self.config[self.config_type]["headers"],
                    params={
                        **self.config[self.config_type].get("params", {}),
                        "offset":
                        current_offset,
                    },
                )
                # Http error handler
                response.raise_for_status()

                response_data = response.json()

                # Set key variable
                match self.config_type:
                    case "meta":
                        key = "dealCustomFieldMeta"
                    case "deal":
                        key = "deals"

                if key not in response_data:
                    print(
                        f"Key '{key}' not found in the response for {self.config_type}"
                    )
                    break

                # Normalizing response data
                normalized_response_data = pd.json_normalize(
                    response_data[key])
                if len(self.config[self.config_type]["schema"]) != 0:
                    df = pd.DataFrame(
                        normalized_response_data,
                        columns=self.config[self.config_type]["schema"].keys(),
                    )
                else:
                    df = pd.DataFrame(normalized_response_data)
                df_list.append(df)

                # Break "while" if offset is more than 500
                if current_offset == 500 or limit == 0:
                    break
                else:
                    current_offset += limit

            # Concat all data frames
            if df_list:
                final_df = pd.concat(df_list, ignore_index=True)
                if not os.path.exists('others'): os.makedirs('others')
                final_df.to_csv("./others/" + key + ".csv",
                                encoding="utf-8-sig",
                                index=False)
            else:
                print("> No data was fetched from the API.\n")
            print(f"> Finished -> {self.config_type}")
        except requests.exceptions.RequestException as req_error:
            print(f"Request error for {self.config_type}: {req_error}")
        except ValueError as val_error:
            print(f"JSON processing error for {self.config_type}: {val_error}")
        except Exception as e:
            print(f"Unexpected error for {self.config_type}: {e}")
