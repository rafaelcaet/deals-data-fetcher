import requests
import pandas as pd
from datetime import datetime
import os
import time


# Cleaning the Screen
def clearScreen():
    os.system("clear")


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
        date_filter = "?filters[created_after]=" + formatted_date

        time.sleep(1)

        df_list = []
        current_offset = 0
        limit = int(self.config[self.config_type]["params"]["limit"])

        try:
            while True:
                # print(
                #     f"\n> (({self.config_type})) fetching data with offset {current_offset} \n"
                # )
                response = requests.get(
                    self.config[self.config_type]["api_url"] + date_filter,
                    headers=self.config[self.config_type]["headers"],
                    params={
                        **self.config[self.config_type].get("params", {}),
                        "offset": current_offset,
                    },
                )
                response.raise_for_status()
                response_data = response.json()

                # Set key variable
                match self.config_type:
                    case "data":
                        key = "dealCustomFieldData"
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
                normalized_response_data = pd.json_normalize(response_data[key])

                if len(self.config[self.config_type]["schema"]) != 0:
                    df = pd.DataFrame(
                        normalized_response_data,
                        columns=self.config[self.config_type]["schema"].keys(),
                    )
                else:
                    df = pd.DataFrame(normalized_response_data)

                df_list.append(df)

                if current_offset == 2000 or limit == 0:
                    break
                else:
                    current_offset += limit

            if df_list:
                final_df = pd.concat(df_list, ignore_index=True)
                # print(
                #     f"> Final {self.config_type} dataframe created with {len(final_df)} rows!\n Exported dataframe to csv!"
                # )
                final_df.to_csv(
                    "./others/" + key + ".csv", encoding="utf-8-sig", index=False
                )
                # final_df.to_excel(key + ".xlsx", index=False, engine="openpyxl")
            else:
                print("> No data was fetched from the API.\n")

        except requests.exceptions.RequestException as req_error:
            print(f"Request error for {self.config_type}: {req_error}")
        except ValueError as val_error:
            print(f"JSON processing error for {self.config_type}: {val_error}")
        except Exception as e:
            print(f"Unexpected error for {self.config_type}: {e}")

        # try:
        #         # type casting to string
        #         dfMeta['id'] = dfMeta['id'].astype(str)
        #         dfData['customFieldId'] = dfData['customFieldId'].astype(str)

        #         # merging dataFrames
        #         df_merged = dfData.merge(dfMeta, left_on='customFieldId', right_on='id', how='left', suffixes=('_dfData', '_dfMeta'))

        #         print('> df_merged dataframe was created!\n')

        #         # pivoting dataFrames
        #         df_pivoted = df_merged.pivot(columns='fieldLabel', values='fieldValue')
        #         df_pivoted = df_pivoted.reset_index()
        #         df_pivoted.columns.name = None

        #     print("> df_pivoted dataframe was created!\n")

        #     Export all dataFrames to csv
        #     print("> Exported all dataframes to csv!")
        #     dfMeta.to_csv(
        #         "csv_dealsCustomFieldMeta.csv", encoding="utf-8-sig", index=False
        #     )
        #     dfData.to_csv(
        #         "csv_dealsCustomFieldData.csv", encoding="utf-8-sig", index=False
        #     )
        #     df_merged.to_csv("csv_merged.csv", encoding="utf-8-sig", index=False)
        #     df_pivoted.to_csv("csv_pivoted.csv", encoding="utf-8-sig", index=False)
        #     df_pivoted.to_excel("csv_pivoted.xlsx", index=False, engine="openpyxl")
        #     print(df_pivoted)
        # except ValueError or KeyError as error:
        #     print(error)
