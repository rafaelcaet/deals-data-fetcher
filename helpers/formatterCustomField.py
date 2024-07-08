import pandas as pd

# from helpers.logger import *


def formatterCustomField():
    """
        formatting df_custom_field_meta and df_custom_field_data in a df_result with the 
        respective dealCustomFieldData and dealCustomFieldMeta
    """
    try:

        ids_field_label = [
            22, 23, 24, 26, 45, 54, 103, 125, 157, 158, 160, 162, 163, 164,
            166, 169, 170, 175, 176, 184, 185, 186, 187, 188, 189, 200, 215,
            216
        ]
        # Open csv's files
        df_custom_field_meta = pd.read_csv('./others/dealCustomFieldMeta.csv')
        df_custom_field_data = pd.read_csv('./others/dealsCustomFieldData.csv')

        # Cria um dicionário de mapeamento de ids para fieldLabel
        field_map = df_custom_field_meta.set_index(
            'id')['fieldLabel'].to_dict()

        # Filtra os ids que estão presentes em ids_field_label
        filtered_field_map = {
            k: v
            for k, v in field_map.items() if k in ids_field_label
        }

        # Adiciona a coluna fieldLabel a df_custom_field_data usando o mapeamento
        df_custom_field_data['fieldLabel'] = df_custom_field_data[
            'customFieldId'].map(filtered_field_map)

        # Lista de fieldLabels que devem estar presentes
        field_labels = list(filtered_field_map.values())

        # Pivotagem inicial do DataFrame
        df_result = df_custom_field_data.pivot_table(
            index=['dealId'],
            columns='fieldLabel',
            values='fieldValue',
            aggfunc='first',
            fill_value='',  # Valor padrão para campos vazios
            dropna=False).reset_index()

        # Verifica colunas faltantes
        missing_columns = [
            label for label in field_labels if label not in df_result.columns
        ]

        # Adiciona colunas faltantes com valores vazios
        for col in missing_columns:
            df_result[col] = ''

        # Reordena as colunas para garantir que estão no mesmo formato
        df_result = df_result[['dealId'] + field_labels]
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


# if __name__ == "__main__":
#     formatterCustomField()
