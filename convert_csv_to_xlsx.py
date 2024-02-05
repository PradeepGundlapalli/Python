import os
import pandas as pd

# Replace 'input.csv' with your CSV file name
csv_file = 'D:/CSV/INVOICE_PROCESSING_INV_DATE_SCAN.csv'

# Replace 'output.xlsx' with your desired XLSX file name
xlsx_file = 'D:/CSV/py/output.xlsx'

# Read CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Write the DataFrame to an Excel file
df.to_excel(xlsx_file, index=False)

print(f'Conversion from CSV to XLSX completed. Output file: {xlsx_file}')