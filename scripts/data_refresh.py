## import libraries
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import numpy as np
import os
import pandas as pd
import requests
import sys
import time

## import configuration
sys.path.append('..')
from config.addresses import addresses

## define etherscan API
etherscan_api_url = 'https://api.etherscan.io/api'
with open('../config/etherscan_api.tkn') as tkn_file:
    etherscan_token = tkn_file.read()

## set gcloud credentials and set up bigquery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../gcloud_service_creds.json'
client = bigquery.Client()

## create table
# define schema
schema = [
    bigquery.SchemaField("Name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("From", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("To", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ContractAddress", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("TransactionHash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("TransactionIndex", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("TokenName", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("TokenSymbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Inbound", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Outbound", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Value", "BIGNUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("SignedValue", "BIGNUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("RunningBalance", "BIGNUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("TransactionFeeEth", "BIGNUMERIC", mode="REQUIRED"),
]

# set project name (created in the console)
project_name = 'banded-charmer-324215'

# create bigquery dataset
dataset_name = 'transactions'
try:
    client.create_dataset(dataset_name)
    print("Created dataset {}.{}".format(project_name, dataset_name))
except Conflict as c:
    print("Dataset {}.{} already exists.".format(project_name, dataset_name))

# create table
table_id = "{}.{}.link_transactions".format(project_name, dataset_name)
table = bigquery.Table(table_id, schema=schema)
try:
    table = client.create_table(table)
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
except Conflict as c:
    print("Table {}.{}.{} already exists.".format(table.project, table.dataset_id, table.table_id))


## get link transactions (API limits up to 10,000 results)
link_transaction_request = '?module=account' \
    '&action=tokentx' \
    '&contractaddress=0x514910771af9ca656af840dff83e8264ecf986ca' \
    '&address={address}' \
    '&page=1' \
    '&offset=10000' \
    '&sort=asc' \
    '&apikey={etherscan_token}'

print("Pulling data from etherscan API...", end="")
start_time = time.time()
link_transactions_df = pd.DataFrame()
for name, address in addresses.items():

    link_transactions = requests.get(
        etherscan_api_url \
        + link_transaction_request.format(
            address=address, etherscan_token=etherscan_token)) \
        .json()['result']
    
    for transaction in link_transactions:
        link_transactions_df = link_transactions_df.append({
            'Name': name,
            'Address': address,
            'From': transaction['from'],
            'To': transaction['to'],
            'TransactionHash': transaction['hash'],
            'TransactionIndex': int(transaction['transactionIndex']),
            'Timestamp': transaction['timeStamp'],
            'Value': transaction['value'],
            'TransactionFeeEth': int(transaction['gasPrice'])*int(transaction['gasUsed']),
            'ContractAddress': transaction['contractAddress'],
            'TokenName': transaction['tokenName'],
            'TokenSymbol': transaction['tokenSymbol'],
            }, ignore_index=True)
    time.sleep(0.2)
end_time = time.time()
print("completed in {:0.2f} minutes".format((end_time-start_time)/60))

# compute direction of transfer (inbound/outbound)
link_transactions_df['Inbound'] = 1*(link_transactions_df['Address'].str.lower() \
                                     == link_transactions_df['To'].str.lower())
link_transactions_df['Outbound'] = 1*(link_transactions_df['Address'].str.lower() \
                                      == link_transactions_df['From'].str.lower())

# convert to proper numerical types and normalize decimals
link_transactions_df['TransactionIndex'] = link_transactions_df['TransactionIndex'].astype(int)
link_transactions_df['Value'] = link_transactions_df['Value'].astype('float64')/10**18
link_transactions_df['TransactionFeeEth'] = link_transactions_df['TransactionFeeEth']/10**18

# compute running balance
link_transactions_df['SignedValue'] = link_transactions_df.apply(
    lambda x: x['Value']*(x['Inbound']-x['Outbound']), axis=1).astype('float64')
link_transactions_df['SignedValueDecimal'] = link_transactions_df['SignedValue']/10**18
link_transactions_df['RunningBalance'] = link_transactions_df.groupby('Name')['SignedValue'].cumsum()
link_transactions_df['RunningBalance'] = link_transactions_df['RunningBalance']

# set dataframe columns to match order of defined schema
link_transactions_df = link_transactions_df[[x.name for x in schema]]

# save dataframe to csv
bq_source_file = '../data/link_transactions_bq.csv'
link_transactions_df.to_csv(bq_source_file, index=False, header=True)

## insert into bigquery table
# create bigquery load job
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True
)

# push to bigquery and print results
start_time = time.time()
print("Pushing data to bigquery...", end="")
with open(bq_source_file, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
job.result() # waits for job to complete
end_time = time.time()
print("completed in {:0.2f} minutes".format((end_time-start_time)/60))
print("Loaded {} rows into {}.{}.{}.".format(
    job.output_rows, table.project, table.dataset_id, table.table_id))


## deduplicate (necessary for ETL cycles until block height limits are set in etherscan API call)
query = """
    CREATE OR REPLACE TABLE `{table_project}.{table_dataset_id}.{table_id}` AS
    SELECT 
        {columns_in_schema}
    FROM (
      SELECT
          *,
         ROW_NUMBER()
              OVER (PARTITION BY Address, `From`, `To`, Value, TransactionHash, TransactionIndex)
              AS rn
      FROM {table_project}.{table_dataset_id}.{table_id}
    )
    WHERE rn = 1
""".format(columns_in_schema=(",").join([f"`{x.name}`" for x in schema]),
           table_project=table.project,
           table_dataset_id=table.dataset_id,
           table_id=table.table_id
          )

query_results = client.query(query)
query_results.result() # wait for job to complete


## get current state of database
start_time = time.time()
print("Pulling data from bigquery...", end="")
query = """
    SELECT * 
    FROM {}.{}.{}
    LIMIT 50000
    """.format(table.project, table.dataset_id, table.table_id)
query_results = client.query(query)

results_df = pd.DataFrame()
for row in query_results.result():
    sample = {}
    for key, value in row.items():
        sample[key] = value
    results_df = results_df.append(sample, ignore_index=True)
end_time = time.time()
print("completed in {:0.2f} minutes".format((end_time-start_time)/60))
print(f"There are a total of {len(results_df)} rows in the table.")