import sys
import os

sys.path.insert(0, os.path.abspath("tmp_libs"))
import pandas as pd
import json

excel_file = r'C:\AI-Metric-Drift-Logical-Duplication-Detection-Engine\Requirements\AI_Rationalization_Demo_Output_Pack (1).xlsx'

try:
    xl = pd.ExcelFile(excel_file)
    schema = {}
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name, header=1)
        schema[sheet_name] = df.columns.tolist()
        
    with open('excel_schema_headers.json', 'w') as f:
        json.dump(schema, f, indent=2)
    print("Proper schema written to excel_schema_headers.json")
except Exception as e:
    print(f"Error reading Excel: {e}")
