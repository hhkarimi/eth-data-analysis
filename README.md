# eth-data-analysis
Project description.

## Running the code


Automate with `cron` by adding the following to `crontab -e`:
```sh
    0 */1 * * * cd /path/to/repo/scripts && /path/to/python data_refresh.py > path/to/logs 2>&1
```
## TODO
* Automate the ETL job in an Airflow DAG
    * Easy to backfill, easy to schedule, easy to turn on/off
* At the start of each job, lookup last block height of a transaction given an account for more efficient data pull
    * Etherscan API for ERC-20 transactions doesn't seem to support this
* Backfill beyond 10,000 transactions
    * Will require to scan through blockchain by block height
* Add transaction fees for token transfers. Need to account multiple transfers in a single token transaction.
    * Internal transaction API is returning empty (https://docs.etherscan.io/api-endpoints/accounts#get-internal-transactions-by-transaction-hash)