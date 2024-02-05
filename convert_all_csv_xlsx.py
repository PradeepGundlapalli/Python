import os
import pandas as pd

# Replace 'input_folder' with the path to your folder containing CSV files
input_folder = 'D:/CSV/1'

# Replace 'output_folder' with the path where you want to save the XLSX files
output_folder = 'D:/CSV/py'

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Get a list of all CSV files in the input folder
csv_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]

# Loop through each CSV file and convert it to XLSX
for csv_file in csv_files:
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(os.path.join(input_folder, csv_file), encoding='ISO-8859-1')

    # Create the output XLSX file name by replacing '.csv' with '.xlsx'
    xlsx_file = os.path.join(output_folder, csv_file.replace('.csv', '.xlsx'))

    # Write the DataFrame to an Excel file
    df.to_excel(xlsx_file, index=False)

    print(f'Conversion from {csv_file} to {xlsx_file} completed.')

print('All conversions completed.')