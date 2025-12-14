import pandas as pd
import sqlite3
import os

# Folder containing your data files
folder_path = r"C:\Users\HP\Downloads\application-data-etl-pipeline"

# Connect to SQLite database
conn = sqlite3.connect("applications.db")

# Loop through all files in the folder
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    
    # Skip if not a supported file type
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in [".csv", ".xls", ".xlsx", ".json"]:
        print(f"Skipping unsupported file: {file_name}")
        continue

    print(f"Processing {file_name} ...")
    
    # Extract: Read file based on its type
    try:
        if file_ext == ".csv":
            data = pd.read_csv(file_path)
        elif file_ext in [".xls", ".xlsx"]:
            data = pd.read_excel(file_path)
        elif file_ext == ".json":
            data = pd.read_json(file_path)
    except Exception as e:
        print(f"Error reading {file_name}: {e}")
        continue

    # Transform: Example transformations
    for col in data.columns:
        if data[col].dtype == object:  # Only transform text columns
            data[col] = data[col].str.strip()  # Remove extra spaces
            data[col] = data[col].str.title()  # Convert text to title case
    
    data = data.drop_duplicates()  # Remove duplicate rows

    # Load: Save into SQLite (table name = filename without extension)
    table_name = os.path.splitext(file_name)[0]
    try:
        data.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"Loaded data into table '{table_name}' successfully!")
    except Exception as e:
        print(f"Error loading {file_name} into database: {e}")

# Close the database connection
conn.close()
print("ETL pipeline executed successfully for all supported files!")
