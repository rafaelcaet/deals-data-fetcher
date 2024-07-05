import requests
import pandas as pd


def fetch_deal_group(api_url, config, results, lock):
    """
        Fetch deal group data
        @Params
            api_url: string,
            config: json
            result: any
            lock: any
    """

    try:
        response = requests.get(
            api_url,
            headers=config["headers"],
        )
        response_data = response.json()

        normalized_response_data = pd.json_normalize(
            response_data["dealGroup"])
        df = pd.DataFrame(
            normalized_response_data,
            columns=config["schema"].keys(),
        )

        with lock:
            results.append(df)

    except requests.exceptions.RequestException as req_error:
        print(f"Request error in dealGroup: {req_error}")
    except ValueError as val_error:
        print(f"JSON processing error for dealGroup: {val_error}")
    except Exception as e:
        print(f"Unexpected error for dealGroup: {e}")
