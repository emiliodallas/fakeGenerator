from google.cloud import bigquery
import re
from collections import defaultdict
import time
import pandas as pd
from pandas_gbq import to_gbq

def transformData(project_id, dataset_id, table_id, new_tableName):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Fetch data from the original table
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query)
    original_data = query_job.result()

    # Create a dictionary to hold aggregated data for each state
    state_data = defaultdict(lambda: defaultdict(int))

    # Iterate through rows and aggregate data by year, grain, and state
    for row in original_data:
        year = row['Year']
        grain = row['Grain']
        for city, value in row.items():
            if city == 'Year' or city == 'Grain':
                continue
            
            state = city[-3:]  # Extract the state abbreviation from the city name
            state_data[(grain, year)][state] += value

    # Create a new schema for the aggregated table
    new_schema = [
        bigquery.SchemaField('grain', 'STRING'),
        bigquery.SchemaField('year', 'INTEGER'),
    ] + [bigquery.SchemaField(state, 'FLOAT') for state in state_data[list(state_data.keys())[0]]]


    # Create a new BigQuery table with the new schema
    new_table = bigquery.Table(f'{project_id}.{dataset_id}.{new_tableName}', schema=new_schema)
    client.create_table(new_table)

    # Insert aggregated data into the new table
    insert_rows = [
        {
            'grain': grain,
            'year': year,
            **state_values
        }
        for (grain, year), state_values in state_data.items()
    ]
    max_retries = 1
    retry_count = 0

    while retry_count < max_retries:
        try:
            errors = client.insert_rows(new_table, insert_rows)
            if not errors:
                print('Data inserted successfully.')
                break  # Exit loop on successful insertion
            else:
                print('Errors:', errors)
                retry_count += 1
        except Exception as e:
            print('An error occurred:', str(e))
            retry_count += 1

    if retry_count == max_retries:
        print('Exceeded maximum retries. Data insertion failed.')

def loadToBQ(uri, dataset_id, table_id):
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField('estacao', 'INTEGER'),
        bigquery.SchemaField('Data', 'DATE'),  # Use DATE data type for the date column
        bigquery.SchemaField('Hora', 'INTEGER'),
        bigquery.SchemaField('Precipitacao', 'FLOAT'),  # Use FLOAT data type for numeric columns
        bigquery.SchemaField('TempBulboSeco', 'FLOAT'),
        bigquery.SchemaField('TempBulboUmido', 'FLOAT'),
        bigquery.SchemaField('TempMaxima', 'FLOAT'),  # Use FLOAT data type for numeric columns
        bigquery.SchemaField('TempMinima', 'FLOAT'),
        bigquery.SchemaField('UmidadeRelativa', 'FLOAT')
    ]
    # Configure the job to load data from Cloud Storage
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,      # Automatically detect schema
        skip_leading_rows=1,  # Skip header row
        field_delimiter=';',  # Use comma as field delimiter

)

    # Load data from Cloud Storage into BigQuery
    table_ref = client.dataset(dataset_id).table(table_id)
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    print(f'Loaded {load_job.output_rows} rows into {table_id}')

def filterCountry(project_id, dataset_id, table_id, new_tableName):
    client = bigquery.Client()

    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE Area = 'Brazil'
    """

    queryJob = client.query(query)
    queryResult = queryJob.result()

    # Specify the destination table in BigQuery
    destination_table = f'{project_id}.{dataset_id}.{new_tableName}'

    # Write the query results to a BigQuery table
    table_ref = client.dataset(dataset_id).table(new_tableName)
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(queryResult.to_dataframe(), table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()

    print(f'Data uploaded to {destination_table}')

def avgTemp(project_id, dataset_id, table_id, new_table):

    # Set up BigQuery client
    client = bigquery.Client()

    # Define your SQL query
    query = f"""
        SELECT TempBulboSeco, TempBulboUmido, TempMaxima, TempMinima
        FROM `{project_id}.{dataset_id}.{table_id}`
    """

    # Run the query and convert results to a DataFrame
    query_job = client.query(query)
    query_results = query_job.result()
    df = query_results.to_dataframe()

    # Calculate the row-wise average for each row
    df['Row_Average'] = df.mean(axis=1)

    # Create a new column 'AvgTemp' and assign the row-wise average values to it
    df['AvgTemp'] = df['Row_Average']

    # Specify the destination table in BigQuery
    destination_table = f'{project_id}.{dataset_id}.{new_table}'

    # Upload the DataFrame to BigQuery
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')

    print(f'Data uploaded to {destination_table}')


#transformData(project_id='dallas-lake', dataset_id='soybitch', table_id='harvested-brazil', new_tableName= 'harvBrazil')
#transformData(project_id='dallas-lake', dataset_id='soybitch', table_id='planted-brazil', new_tableName= 'plantBrazil')
#transformData(project_id='dallas-lake', dataset_id='soybitch', table_id='production-brazil', new_tableName= 'produBrazil')
#transformData(project_id='dallas-lake', dataset_id='soybitch', table_id='yield-brazil', new_tableName= 'yielBrazil')

#loadToBQ(path='gs://covid-bucketo/soybitch/Inputs_Pesticides_Use_E_All_Data_(Normalized).csv', dataset_id='soybitch', table_id='pesticide-brazil')
loadToBQ(uri='gs://covid-bucketo/soybitch/treated/weather_stations_codes.csv-00000-of-00001', 
         dataset_id='soybitch', 
         table_id='weatherCodes-brazil')

#loadToBQ(uri='gs://covid-bucketo/soybitch/treated/conventional_weather_stations_inmet_brazil_1961_2019.csv-00000-of-00001', 
         #dataset_id='soybitch', 
         #table_id='weather-brazil')

#avgTemp(project_id='dallas-lake', dataset_id='soybitch', table_id='weather-brazil', new_table= 'weatBrazil')


#filterCountry(project_id='dallas-lake', dataset_id='soybitch', table_id='pesticide-brazil', new_tableName= 'pestBrazil')