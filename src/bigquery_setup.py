"""
BigQuery Setup for Restaurant Data Analysis

This module sets up BigQuery connections and loads transformed data for restaurant analysis.
It connects to an existing dataset and loads data into existing tables from CSV files.

Author: Justine Paul Padayao
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import os


def setup_credentials(key_path):
    """
    Set up credentials for BigQuery access.

    Parameters
    ----------
    key_path : str
        Path to the service account JSON key file.

    Returns
    -------
    google.oauth2.service_account.Credentials
        Credentials object for BigQuery authentication.
    """
    return service_account.Credentials.from_service_account_file(key_path)


def create_bigquery_client(credentials):
    """
    Create a BigQuery client.

    Parameters
    ----------
    credentials : google.oauth2.service_account.Credentials
        Credentials object for BigQuery authentication.

    Returns
    -------
    google.cloud.bigquery.client.Client
        BigQuery client object.
    """
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def load_table_from_csv(client, dataset_id, table_name, csv_file):
    """
    Load data into an existing BigQuery table from a CSV file.

    Parameters
    ----------
    client : google.cloud.bigquery.client.Client
        BigQuery client object.
    dataset_id : str
        ID of the dataset containing the table.
    table_name : str
        Name of the table to load data into.
    csv_file : str
        Path to the CSV file containing the data to be loaded.

    Returns
    -------
    None
    """
    table_id = f"{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}")


def main():
    """
    Main function to orchestrate the BigQuery setup process.

    This function sets up credentials, creates a BigQuery client,
    and loads data from CSV files into existing BigQuery tables.

    Returns
    -------
    None
    """
    credentials = setup_credentials('../config/bigquery_credentials.json')
    client = create_bigquery_client(credentials)

    dataset_id = "bigquery-dataengineering.tablecheck_data"

    tables = ['cleaned_data', 'restaurant_stats', 'customer_stats',
              'popular_dishes', 'profitable_dishes', 'frequent_visitors']

    for table_name in tables:
        csv_file = f"tmp/{table_name}.csv"
        if os.path.exists(csv_file):
            load_table_from_csv(client, dataset_id, table_name, csv_file)
        else:
            print(f"Warning: CSV file for {table_name} not found.")

    print("BigQuery data loading complete.")


if __name__ == "__main__":
    main()
