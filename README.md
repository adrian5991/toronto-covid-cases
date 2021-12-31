# toronto-covid-cases

Data retrieved from: https://open.toronto.ca/dataset/covid-19-cases-in-toronto/

## Visualization

https://datastudio.google.com/reporting/7780fef9-2ebc-41fb-a31b-1568fa4392a3

### Description
1. `python/data_to_gcp.py` hits REST API and uploads data to Google Cloud Storage.
2. The function in `python/main.py` is triggered upon upload to Cloud Storage to move the data to BigQuery.
    - the data stays denormalized as a flat table
3. dbt makes simple transformations to create two models in BigQuery for use in Data Studio

### Improvements
- currently, duplicate data can be inserted into the staging table in BQ. dbt deduplicates the data when creating the models but the staging table has to be manually cleaned. Can schedule the deduplication query or add it to the event trigger function
- script to pull data from REST API could be automated with cron, Airflow, etc.,
- dbt has to be run manually to refresh models; can also automate with cron, Airflow, etc.,
- add a few simple tests in dbt