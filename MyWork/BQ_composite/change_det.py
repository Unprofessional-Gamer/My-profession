from google.cloud import bigquery
import pandas as pd

def load_existing_data_from_bq(table_id):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Query to load existing data from BigQuery
    query = f"SELECT * FROM `{table_id}`"
    existing_data = client.query(query).to_dataframe()
    
    return existing_data

def load_new_data_from_csv(file_path):
    # Load new data from CSV
    new_data = pd.read_csv(file_path)
    return new_data

def create_composite_key(df, key_columns, key_name):
    df[key_name] = df[key_columns].astype(str).agg('-'.join, axis=1)
    return df

def detect_changes_and_prepare_update(existing_data, new_data, key_columns):
    # Create composite keys
    existing_data = create_composite_key(existing_data, key_columns, 'composite_key_existing')
    new_data = create_composite_key(new_data, key_columns, 'composite_key_new')
    
    # Merge data on composite key to find differences
    merged_data = pd.merge(existing_data, new_data, left_on='composite_key_existing', right_on='composite_key_new', how='outer', suffixes=('_old', '_new'), indicator=True)
    
    # Identify updated and new records
    updated_records = merged_data[merged_data['_merge'] == 'both']
    new_records = merged_data[merged_data['_merge'] == 'right_only']
    
    # For updated records, check for changes in columns
    updated_columns = []
    for col in new_data.columns:
        if col != 'composite_key_new' and col in updated_records.columns:
            updated_columns.append(updated_records[col + '_old'] != updated_records[col + '_new'])
    
    # Combine conditions to find changed records
    if updated_columns:
        changed_records = updated_records[updated_columns[0]]
        for condition in updated_columns[1:]:
            changed_records = changed_records | condition
    else:
        changed_records = pd.DataFrame()

    # Combine changed and new records
    updated_and_new_records = pd.concat([updated_records[changed_records], new_records])
    
    # Drop old columns and keep new ones for the final DataFrame
    final_data = updated_and_new_records.drop([col for col in updated_and_new_records.columns if col.endswith('_old')], axis=1)
    
    # Rename columns to original names
    final_data.columns = final_data.columns.str.replace('_new', '')
    
    return final_data, updated_and_new_records

def log_changes(changed_records, log_file_path):
    # Detect column changes
    column_changes = {}
    for col in changed_records.columns:
        if col.endswith('_old'):
            col_base = col[:-4]
            column_changes[col_base] = changed_records[[col, col_base + '_new']]
    
    # Log column changes
    with open(log_file_path, 'w') as log_file:
        for col, changes in column_changes.items():
            log_file.write(f"Changes in {col}:\n")
            log_file.write(changes.to_string(index=False))
            log_file.write('\n\n')

def update_bigquery_table(final_data, table_id):
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Load the updated data to BigQuery
    job = client.load_table_from_dataframe(final_data, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    job.result()  # Wait for the job to complete

def main(table_id, csv_file_path, key_columns, log_file_path):
    # Load existing and new data
    existing_data = load_existing_data_from_bq(table_id)
    new_data = load_new_data_from_csv(csv_file_path)
    
    # Detect changes and prepare data for update
    final_data, changed_records = detect_changes_and_prepare_update(existing_data, new_data, key_columns)
    
    # Log changes
    log_changes(changed_records, log_file_path)
    
    # Update BigQuery table if there are changes
    if not final_data.empty:
        update_bigquery_table(final_data, table_id)
        print("Data updated successfully in BigQuery.")
    else:
        print("No changes detected, no update necessary.")

if __name__ == "__main__":
    # Define placeholders
    table_id = 'your_project.your_dataset.your_table'  # Replace with your table ID
    csv_file_path = 'path_to_your_new_data.csv'  # Replace with your CSV file path
    key_columns = ['column1', 'column2', 'column3']  # Replace with your key columns
    log_file_path = 'column_changes.log'  # Path to log file
    
    # Run main function
    main(table_id, csv_file_path, key_columns, log_file_path)
