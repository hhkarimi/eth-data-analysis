# eth-data-analysis
Project description.

## Running the code

## TODO
* Automate the ETL job in an Airflow DAG
    * Easy to backfill, easy to schedule, easy to turn on/off
* At the start of each job, lookup last block height of a transaction given an account for more efficient data pull
* Backfill beyond 10,000 transactions
    * Will require to scan through blockchain by block height
* Add transaction fees for token transfers. Need to account multiple transfers in a single token transaction.
    * Internal transaction API is returning empty (https://docs.etherscan.io/api-endpoints/accounts#get-internal-transactions-by-transaction-hash)