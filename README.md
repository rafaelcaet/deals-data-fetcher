### Deals Data Fetcher

This project contains a script to fetch data of DEALS that have been updated from `31/12/2023` onwards through an API.

### Initial Setup

Before running the script, you need to set up a config.json file in the project's root folder. This file must contain the necessary information to connect to the API. Below is a template of config.json that should be filled in with the correct information:

*config.json*

```json
{
    "Deal": {
        "api_url": "https://johndoe.api.com/",
        "headers": {
            "accept": "application/json",
            "Api-Token": "johndoe123@321"
        },
        "schema": {
            "id": "str",
            ...
        }
    }
}
```

### Field Descriptions

*api_url* : API URL to fetch DEALS data.

*headers*: Headers required for authentication and API response formatting.
*accept*: Accepted content type, usually application/json.

*Api-Token*: Authentication token for API access. Replace "johndoe123@321" with your valid token.

*schema*: Expected schema for DEALS data. You should complete the schema as needed, including the fields you expect to receive and their data types.

### Running the Script

After configuring config.json, you can execute the main script to fetch the data. Follow the steps below:

1. Ensure that the config.json file is correctly filled out and saved in the project's root folder.
2. Run the main.py script with the following command:

```bash
python3 main.py
```
### Requirements

Python 3+: Make sure you have the correct version of Python installed.

Dependencies: Install any necessary dependencies using the following command (if applicable):

*pandas*
```bash
pip install -r pandas
```

*requests*
```bash
pip install -r requests
```
### Notes
API Token Security: The API token is sensitive and should be kept secure. Do not share your config.json with third parties.
Schema: The schema should reflect the expected data structure from the API. Review and adjust as necessary to ensure data integrity.