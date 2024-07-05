import pandas as pd

# from helpers.logger import *


def formatterCustomField():
    """
        formatting dfCustomFieldMeta and dfCustomFieldData in a df_result with the 
        respective dealCustomFieldData and dealCustomFieldMeta
    """
    try:

        ids_field_label = [
            22, 23, 24, 26, 45, 54, 103, 125, 157, 158, 160, 162, 163, 164,
            166, 169, 170, 175, 176, 184, 185, 186, 187, 188, 189, 200, 215,
            216
        ]
        # Open csv's files
        dfCustomFieldMeta = pd.read_csv('./others/dealCustomFieldMeta.csv')
        dfCustomFieldData = pd.read_csv('./others/dealCustomFieldData.csv')

        field_map = dfCustomFieldMeta.set_index('id')['fieldLabel'].to_dict()
        filtered_field_map = {
            k: v
            for k, v in field_map.items() if k in ids_field_label
        }
        dfCustomFieldData['fieldLabel'] = dfCustomFieldData[
            'customFieldId'].map(filtered_field_map)

        present_columns = dfCustomFieldMeta['fieldLabel'].unique()
        columns_to_pivot = list(filtered_field_map.values())
        columns_to_pivot_filtered = [
            col for col in columns_to_pivot if col in present_columns
        ]

        if set(columns_to_pivot_filtered) == set(columns_to_pivot):
            df_result = dfCustomFieldData.pivot_table(
                index=['dealId'],
                columns='fieldLabel',
                values='fieldValue',
                aggfunc='first',
                fill_value=None,
                dropna=False).reset_index()

            df_result = df_result[['dealId'] +
                                  sorted(filtered_field_map.values())]
            df_result.to_csv("./others/resultCustomFields.csv",
                             encoding="utf-8-sig",
                             index=False)
            df_result.to_excel("./others/resultCustomFields.xlsx",
                               index=False,
                               engine="openpyxl")
    except KeyError as e:
        print(f"Error: pivoting error: {e}")
    except FileNotFoundError as e:
        print(f"Error: file not found: {e}")
    except pd.errors.EmptyDataError as e:
        print(f"Error: empty data: {e}")
    except pd.errors.ParserError as e:
        print(f"Error: parser error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
