import subprocess
import sys
import os

try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "openpyxl", "--target", "tmp_libs", "--quiet"])
    sys.path.insert(0, os.path.abspath("tmp_libs"))
except Exception as e:
    print(f"Failed to install: {e}")

try:
    import pandas as pd
    import json
    excel_file = r'C:\AI-Metric-Drift-Logical-Duplication-Detection-Engine\Requirements\AI_Rationalization_Demo_Output_Pack (1).xlsx'
    
    xl = pd.ExcelFile(excel_file)
    schema = {}
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        schema[sheet_name] = df.columns.tolist()
        
    with open('excel_schema.json', 'w') as f:
        json.dump(schema, f, indent=2)
    print("Schema written to excel_schema.json")
except Exception as e:
    print(f"Error reading Excel: {e}")
