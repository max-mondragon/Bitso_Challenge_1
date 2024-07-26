import time
import datetime
import requests
import hmac
import hashlib
import json
import threading
import random
import websocket
import os


from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, unix_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

#########################################################
# Bitso Spread Monitor                                  #
# ----------------------------------------------------- #
# Created by: Max Mondragon                             #
# 25-July-2024                                          #
#########################################################


# API Key and Secret. Protect accordingly
# environment.get("api-key")
# environment.get("api-secret")
api_key = '____Ta___V'
api_secret = '______25__ae__d3__f1__ee__73____'

# Variables required globally
cache_data = {}
lock_update = False
sockets = []

spark = SparkSession.builder.appName("BitsoSpreadMonitorApp").getOrCreate()
# Schema used for the final table
schema = StructType([
    StructField("orderbook_timestamp", StringType(), True),
    StructField("book", StringType(), True),
    StructField("bid", FloatType(), True),
    StructField("ask", FloatType(), True),
    StructField("spread", FloatType(), True)
])


# Creates a new subscription to Websocket
def subscribe_bitso_data(book):
    # If it is already subscribed, ignore
    if (book in cache_data):
        if ("Subscribed" in cache_data[book]):
            if (cache_data[book]["Subscribed"]):
                return
    try:
        # Created as a new thread, to listen asynchronous
        print("Attempting to subscribe to " + book)
        new_thread = threading.Thread(
            target=create_websocket_connection,
            args=(book,),
            name="Async socket for " + book,
        )
        new_thread.start()

    except Exception as e:
        print("Fatal error: Could not subscribe to " + book)
        print(e)
        raise(e)


# Creates the Websocket connection to a Book
def create_websocket_connection(book):
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        "wss://ws.bitso.com",
        on_open=lambda *x: on_websocket_open(book, *x),
        on_message=lambda *x: on_websocket_message(book, *x),
        on_error=lambda *x: on_websocket_error(book, *x),
        on_close=lambda *x: on_websocket_close(book, *x)
    )
    sockets.append(ws)
    ws.run_forever()  # Listens until stopped.


# on_event for web socket calls
def on_websocket_open(book, ws):
    sub_params = {'action': 'subscribe', 'book': book, 'type': 'orders'}
    ws.send(json.dumps(sub_params))
    print("Subscription to " + book + " open.")
    if (book not in cache_data):
        cache_data[book] = {
            "Content": '',
            "LastUpdate": datetime.datetime.utcnow(),
            "Subscribed": True
        }


def on_websocket_message(book, ws, message):
    if (lock_update):
        # If there is a reading in progress, discard update.
        print("Data being read")
        return
    try:
        data = json.loads(message)
        if ("payload" not in data):
            # print("Bad data:" + str(data))
            # print(data)
            return
        cache_data[book] = {
            "Content": data,
            "LastUpdate": datetime.datetime.utcnow(),
            "Subscribed": True
        }
    except Exception as e:
        print(e)
        time.sleep(1)


def on_websocket_error(book, ws, error):
    print("Book " + book + " subscription error: " + error)


def on_websocket_close(book, status, message, responseQ):
    print("Subscription to " + book + " closed.")
    if (book in cache_data):
        if ("Subscribed" in cache_data[book]):
            cache_data[book]["Subscribed"] = False


# Requests data directly from API
def request_bitso_api(book):
    # This method should only be used the first time
    # Constantlyt polling data results in Too Many Requests error.

    nonce = str(int(round(time.time() * 1000000)))
    method = 'GET'
    url = 'https://api.bitso.com'
    request_path = '/api/v3/order_book/'
    param = "?" + "book=" + book

    # Creates contatenated string
    data = nonce + method.upper() + request_path + param
    # Creates signature, using api_secret as key
    signature = hmac.new(
        api_secret.encode('utf-8'),
        data.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    # Creates the header
    header = {'Authorization': 'Bitso %s:%s:%s' % (api_key, nonce, signature)}

    # Tries to get the data
    attemps = 3
    while (attemps > 0):
        try:
            response = requests.get(url + request_path + param, headers=header)
            break
        except Exception as e:
            print(e)
            attemps -= 1
            time.sleep(random.random())  # backoff 0-1 second

    if response.status_code == 200:
        json_data = response.json()
        # Since data comes from two different calls (API call and Webhook),
        # the structure is slightly different on column names
        # This replaces "price" key with "v"
        if ("payload" in json_data):
            if ("bids" in json_data["payload"]):
                for elem in json_data["payload"]["bids"]:
                    elem['r'] = elem.pop('price')
            if ("asks" in json_data["payload"]):
                for elem in json_data["payload"]["asks"]:
                    elem['r'] = elem.pop('price')

        # If called again, maintain the subscribed flag as it was
        subscribed = False
        if (book in cache_data):
            if ("Subscribed" in cache_data[book]):
                subscribed = cache_data[book]["Subscribed"]
        cache_data[book] = {
            "Content": json_data,
            "LastUpdate": datetime.datetime.utcnow(),
            "Subscribed": subscribed
        }
        return json_data
    else:
        print(response.json())
        return []


# This function returns the data in its most up to date form
# Instead of polling, the websocket keeps it updated
def fetch_data(book):
    lock_update = True
    data_return = ''
    if (book not in cache_data or cache_data[book]["Content"] == ''):
        # Data may still not be available, wait
        wait_time = 60
        while (wait_time > 0 and
                (book not in cache_data or cache_data[book]["Content"] == '')):
            print("No data, waiting " + str(wait_time) + " seconds...")
            lock_update = False
            time.sleep(1)
            wait_time -= 1
            lock_update = True
        if (book not in cache_data or cache_data[book]["Content"] == ''):
            raise Exception("Fatal error: no data retrieved")
        data_return = cache_data[book]["Content"]
    else:
        # Refresh data
        interval = datetime.datetime.utcnow() - cache_data[book]["LastUpdate"]
        if (interval.total_seconds() >= 60):
            # Updates are subscribed, but no changes may have been done
            # if a minute has passed without changes, force update.
            print("Cache data is old. Updating...")
            request_bitso_api(book)

        data_return = cache_data[book]["Content"]

    lock_update = False
    return data_return


# Utility function to save a dataframe as csv
def save_to_disk(df, path):
    print("Saving... " + path + "")
    df.coalesce(1).write.csv(path)
    print("Saved: " + path + " successfuly.")


# Main script
emp_RDD = spark.sparkContext.emptyRDD()
main_df = spark.createDataFrame(data=emp_RDD, schema=schema)

# Books to check
# Multiple books can be run simultaneously
# books = ['btc_mxn', 'usd_mxn']
books = ['btc_mxn']

data_samples = 600  # Number of samples, 1 each second
counter = 0  # Current number of samples
global_counter = 0
exit_process = False

while not exit_process:
    # Timer to identify delays
    time_0 = datetime.datetime.utcnow()
    counter = 0
    while (counter < data_samples):
        time_1 = datetime.datetime.utcnow()
        for book in books:

            # If book is not monitored, add
            if (book not in cache_data):
                print(book)
                request_bitso_api(book)
                subscribe_bitso_data(book)

            # Recover data for this book
            data = fetch_data(book)
            time_string = time_1.strftime("%Y-%m-%dT%H:%M:%S+00:00")

            try:
                # Prepare data
                bids_df = spark.createDataFrame(data["payload"]["bids"])
                asks_df = spark.createDataFrame(data["payload"]["asks"])
                # Cast strig column to Double, to avoid max/min on texts
                bids_df = bids_df.withColumn(
                    "price", bids_df["r"].cast(FloatType())
                )
                asks_df = asks_df.withColumn(
                    "price", asks_df["r"].cast(FloatType())
                )

                # Calculate best_bid, best_ask and spread
                # (best_ask - best_bid) * 100/ best_ask
                max_bid = bids_df.agg({"price": "max"}).collect()[0][0]
                min_ask = asks_df.agg({"price": "min"}).collect()[0][0]
                spread = (min_ask - max_bid) * 100 / min_ask

                # Append data to Main DataFrame
                temp_df = spark.createDataFrame(
                    [[time_string, book, max_bid, min_ask, spread]],
                    schema
                )
                main_df = main_df.union(temp_df)

            except Exception as e:
                print(e)
                print(data)
                raise(e)

        # Print a dot every 30 seconds
        if (counter % 30 == 0):
            print('.', end='')

        counter += 1
        # Data should be consulted every second
        # this tries to fix any delay due to processing or network lag
        time_2 = datetime.datetime.utcnow()
        delay = (time_2 - time_1).total_seconds()
        time.sleep(min(max(1 - delay, 0), 1))  # Fetch data every 1 seconds

    # Max number of samples collected
    if (counter == data_samples):

        for book in books:

            start_date = datetime.datetime(
                time_0.year,
                time_0.month,
                time_0.day
            )
            time_since = time_0 - start_date
            minutes_since = int(time_since.total_seconds() / 60)

            # Filter data for each book
            result_df = main_df.where("book = '" + book + "'")
            # Partitioning is: AppName/date=yyyymmdd/order_book=?/minutes=?/
            # Minutes is equal to the number of minutes past midnigth
            # this is useful if we want to query data for longer periods.
            # for example:
            # Where minutes <= 300 will bring everythig before 5 am. (5 x 60)
            path = '/%s/%s/%s/%s' % (
                "BitsoSpreadMonitor",
                "date=" + time_0.strftime("%Y%m%d"),
                "order_book=" + book,
                "minutes=" + str(minutes_since)
            )

            cwd = os.getcwd()
            # save_to_disk(result_df, cwd + path)
            new_thread = threading.Thread(
                target=save_to_disk,
                args=(result_df, cwd + path),
                name="Async file save",
            )
            new_thread.start()

        main_df = spark.createDataFrame(data=emp_RDD, schema=schema)
    global_counter += 1
    if (global_counter >= 5):
        # If limited, turn off the websockets
        for w in sockets:
            w.keep_running = False
        exit_process = True

print("Process finished")
