## Imports
from pprint import pprint
import datetime as DT
import numpy as np
import math

import helpers

## Constants
TIME_BAR_SIZE = DT.timedelta(minutes=1) # 1 minute

## Get Data from a file
data = {}
with open('test_data.xc') as file:
    for line in file:
        if 'str' in line:
            break

        ticker = line[0:9].strip()
        if data.get(ticker) == None:
            data[ticker] = []

        # Parse date time. Assuming date -> YYYYMMDD, time -> decimal(seconds)
        date_parsed = helpers.string_date_to_python_date(line[16:26].strip())
        time_parsed = helpers.decimal_time_to_python_time(line[26:41].strip())
        parsed_dt = DT.datetime.combine(date_parsed, time_parsed)

        price = float(line[50:70].strip())
        volume = int(line[70:90].strip())

        data[ticker].append([parsed_dt, price, volume])

#pprint(data)

## Aggregate data in TIME_BAR_SIZE chunks
aggr_data = {}
for ticker in data:
    aggr_data[ticker] = {
        'num_elements': 0,
        'price': [],
        'volume': [],
        'start_time': [], # TODO - Remove this if we don't need it
    }
    transactions = data[ticker]
    len_transactions = len(transactions)
    index = 1
    price = transactions[0][1]
    volume = transactions[0][2]
    start_time = helpers.round_time(transactions[10][0], TIME_BAR_SIZE)
    while (index < len_transactions):
        transaction = transactions[index]
        if transaction[0] <= start_time + TIME_BAR_SIZE:
            price += transaction[1]
            volume += transaction[2]
            index += 1
        else:
            aggr_data[ticker]['num_elements'] += 1
            aggr_data[ticker]['start_time'].append(start_time)
            aggr_data[ticker]['price'].append(price)
            aggr_data[ticker]['volume'].append(volume)

            price = 0.0
            volume = 0
            start_time += TIME_BAR_SIZE

# pprint(aggr_data)

## Calculate standard deviation, stdev across aggregates
for ticker, data in aggr_data.iteritems():
    # Volume weighted average
    average = np.average(data['price'], weights=data['volume'])
    variance = np.average((data['price']-average)**2, weights=data['volume'])
    data['stdev'] = math.sqrt(variance)

pprint(aggr_data)

## Compute delta(P), price difference

## Expand no of observations

## Classify buy / sell volume (loop)

## Calculate VPIN
