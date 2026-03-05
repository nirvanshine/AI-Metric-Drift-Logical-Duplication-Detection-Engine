import sys, os
sys.path.insert(0, os.path.abspath("tmp_libs"))
import pandas as pd
import json

excel_file = r'C:\AI-Metric-Drift-Logical-Duplication-Detection-Engine\Requirements\AI_Rationalization_Demo_Output_Pack (1).xlsx'

xl = pd.ExcelFile(excel_file)
output = {}

for sheet_name in xl.sheet_names:
    df = xl.parse(sheet_name, header=None)
    rows = []
    for _, row in df.iterrows():
        row_data = [str(v) if pd.notna(v) else "" for v in row]
        rows.append(row_data)
    output[sheet_name] = rows

with open('excel_full_content.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"Extracted {len(xl.sheet_names)} sheets to excel_full_content.json")
for s in xl.sheet_names:
    print(f"  {s}: {len(output[s])} rows")
