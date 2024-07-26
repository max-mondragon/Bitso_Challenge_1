# Welcome to my Bitso Challenge!

Hello!
Here is the documentation for my Bitso Challenge 1.
This challenge consists on the calculation of the spread based on best ask and best bid, obtained from the Bitso API.
Data should be generated every 1 second during periods of 10 minutes. Then exported as CSV file.
Created by: **Max Mondragon**.


## API Requests

First of all, I started my tests with the stage environment (stage.bitso) and after being satisfied, moved to the official one (api.bitso).
The solution uses 2 methods to recover the data:
1) HTTP request to Orders API endpoint, this one works but is not suitable for polling every second due to throttling.
So its only used for the initial data recovery and 1 minute updates if data has not been changed by 2.
2) Websocket API for Orders Channel. This subscription model is more efficient since updates are pushed when they happen. This way I can keep a copy of relevant data all the time.

Method 1 required a signed petition, so api_key and api_secret should be provided and protected accordingly, for example as Airflow variables/connections.
If Method 1 fails, the process waits a maximum of 60 seconds for data refresh from the Websocket, so its not strictly mandatory as long as data is moving and being pushed.

## Installation

The main script is written in Python, using Spark for data transformations and loading.
A personal computer with reduced capacity was used for the tests, with a Docker image for Spark with Jupyter notebooks for testing. Docker image is available [Here](https://hub.docker.com/r/jupyter/all-spark-notebook).
Using "docker-compose up -d" and accessing Jupyter through http://127.0.0.1:8888/ we can easily run the script on a notebook.

## Code and processing

Since the data capture should be done each second, the program does a best effort to recover data with this interval, I included a delay check so instead of just waiting 1 second, should lag happen, considers the correct milliseconds correction.
It also uses multithreading for time consuming tasks, like unifying a DataFrame and saving to disk.
For a more robust solution, Kafka or other producer may be considered, this way we can ensure data is not lost if the consumer lags or stops.

Airflow and a DAG that runs every 10 minutes was considered, but my PC could barely handle the Spark image and, after testing Airflow, it simple couldn't keep the services running.

The code has been checked to be PEP8 compliant.

## Partitioning

Data is collected and, after 600 samples (1 each second) it is loaded into the "Data Lake", simulating S3 / Hive partitioning.
The data is saved using the following partitions:
 > /app_name/date=yyyymmdd/order_book=?/minutes=?/

For example:
 > /BitsoSpreadMonitor/date=20240725/order_book=mxn_btc/minutes=10/

- The application name is just the root folder.
- The field "date" enables daily data partitioning, specially useful for range queries.
- The field "order_book" separates data between books needed, mxn_btc and usd_mxn in this case.
- Finally "minutes" represents the starting point of data collection in minutes from midnight. It separates each batch run and is useful if we want to recover data for periods.

And example of this partitioning would be something like:
 > SELECT *
FROM BitsoSpreadMonitor
WHERE date = 20240725
AND order_book = mxn_btc
AND minutes <= 300

This would bring all the data collected for 25-July-2024, book mxn_btc from 00:00 to 05:00 (300 minutes).

A consideration was made for another partitioning schema, like /year=?/month=?/day=/.
Even if this partitioning has some advantages, it also makes some queries considerably more complex.
For example data from 03-may-2024 to 28-apr-2024, we would need to juggle with the 3 fields. Contrary to the chosen one where is as simple as >= and <=. 


## Results

The program was left running for 1 hour, so 6 batches of 10 minutes data with 600 samples each one, for each coin, are available.

