from google.cloud import bigquery
import pandas as pd

# Your Google Cloud project ID
project_id = 'your_project_id'

# Create a BigQuery client using the default application credentials
client = bigquery.Client(project=project_id)

# Define your query
query = """
SELECT column1, column2, column3
FROM `your_project_id.your_dataset.your_table`
WHERE some_column = 'some_value'
"""

# Execute the query
query_job = client.query(query)

# Get the result as a DataFrame
df = query_job.to_dataframe()

# Save the DataFrame to a CSV file
csv_file_path = 'output.csv'
df.to_csv(csv_file_path, index=False)

print(f"Data has been saved to {csv_file_path}")
