import requests
import pandas as pd
import re


def fetch_deal_stage(api_url, config, results, lock):
    """
        Fetch deal stage data
        @Params
            api_url: string
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
            response_data["dealStage"])

        df = pd.DataFrame(
            normalized_response_data,
            columns=config["schema"].keys(),
        )

        match = re.search(r'/deals/(\d+)', api_url)
        df['dealId'] = match.group(1)

        with lock:
            results.append(df)

    except requests.exceptions.RequestException as req_error:
        print(f"Request error in dealstage: {req_error}")
    except ValueError as val_error:
        print(f"JSON processing error for dealstage: {val_error}")
    except Exception as e:
        print(f"Unexpected error for dealstage: {e}")


if __name__ == "__main__":
    fetch_deal_stage()
