# toronto-covid-cases

Data retrieved from: https://open.toronto.ca/dataset/covid-19-cases-in-toronto/

## Visualization

https://datastudio.google.com/reporting/7780fef9-2ebc-41fb-a31b-1568fa4392a3

### Description
1. `python/data_to_gcp.py` hits REST API and uploads data to Google Cloud Storage.
2. The function in `main.py` is triggered upon upload to Cloud Storage to move the data to BigQuery.
3. dbt makes simple transformations to create two models in BigQuery for use in Data Studio
