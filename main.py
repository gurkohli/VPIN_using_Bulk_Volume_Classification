## Imports
from pprint import pprint
from scipy.stats import norm
import plotly
import plotly.graph_objs as go
import datetime as DT
import numpy as np
import math
import csv

import helpers

## Constants
FILE_NAME = 'ES20100506.xc'
IS_DATA_AGGREGATED = True
IS_PRICE_DIFFERENCED = True
IS_VPIN_ROLL_FORWARD = True
TIME_BAR_SIZE = DT.timedelta(minutes=1) # 1 minute
BUCKET_SIZE_MAP = {
    'A': 43959,
    'AAAP': 1349,
    'AAC': 2634,
    'ESM10': 31106,
} # Temporary var. TODO: Get this from file
BUCKETS_PER_ITER = 50

VERBOSE = True;
VERBOSE_CSV_FILE_NAME = "verbose_data.csv"
## Get Data from a file
print 'Parsing data from file: ',
data = {}
with open(FILE_NAME) as file:
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
print('Complete')
# pprint(data)

aggr_data = {}
print 'Aggregating Data: ',
if (IS_DATA_AGGREGATED == False):
    for ticker in data:
        aggr_data[ticker] = {
            'num_elements': len(data[ticker]),
            'price': [],
            'volume': [],
            'start_time': [], # TODO - Remove this if we don't need it
        }
        for datum in data[ticker]:
            aggr_data[ticker]['start_time'].append(datum[0])
            aggr_data[ticker]['price'].append(datum[1])
            aggr_data[ticker]['volume'].append(datum[2])
    print('Skipped')
else:
    ## Aggregate data in TIME_BAR_SIZE chunks
    for ticker in data:
        aggr_data[ticker] = {
            'num_elements': 0,
            'price': [],
            'volume': [],
            'start_time': [], # TODO - Remove this if we don't need it
            'num_trades_in_bar': [],
        }
        transactions = data[ticker]
        len_transactions = len(transactions)
        index = 1
        price = transactions[0][1]
        volume = transactions[0][2]
        start_time = helpers.round_time(transactions[10][0], TIME_BAR_SIZE)
        num_trades_in_bar = 1;
        while (index < len_transactions):
            transaction = transactions[index]
            if transaction[0] <= start_time + TIME_BAR_SIZE:
                price += transaction[1]
                volume += transaction[2]
                num_trades_in_bar += 1;
                index += 1
            else:
                aggr_data[ticker]['num_elements'] += 1
                aggr_data[ticker]['start_time'].append(start_time)
                aggr_data[ticker]['price'].append(price)
                aggr_data[ticker]['volume'].append(volume)
                aggr_data[ticker]['num_trades_in_bar'].append(num_trades_in_bar)
                price = 0.0
                volume = 0
                num_trades_in_bar = 0
                start_time += TIME_BAR_SIZE
    print('Complete')
# pprint(aggr_data["ESM10"]["num_elements"])

## Calculate delta(P), price difference between aggregated time bars
print 'Calculating Price Difference: ',
if IS_PRICE_DIFFERENCED:
    for ticker, data in aggr_data.iteritems():
        num_time_bars = data['num_elements']
        data['price_diff'] = [data['price'][0]];
        for i in range(1, num_time_bars):
            data['price_diff'].append(data['price'][i] - data['price'][i-1])

    data['price'] = data['price_diff']
    print('Complete')
else:
    print('Skipped')

## Calculate standard deviation, stdev across aggregates
print 'Calculating Standard Deviation: ',
for ticker, data in aggr_data.iteritems():
    # Volume weighted average
    average = np.average(data['price'], weights=data['volume'])
    variance = np.average((data['price']-average)**2, weights=data['volume'])
    data['stdev'] = math.sqrt(variance)

print('Complete')
#pprint(aggr_data['A'])

## Expand no of observations

## Classify buy / sell volume (loop)
print 'Classifying Volume: ',
for ticker, data in aggr_data.iteritems():
    if ticker not in BUCKET_SIZE_MAP:
        # continue # TODO: Do something with this
        size = input('Enter Bucket Size for ' + ticker + ': ')
        if size <= 0:
            continue
        BUCKET_SIZE_MAP[ticker] = size
    BUCKET_SIZE = BUCKET_SIZE_MAP[ticker]
    len_time_bars = data['num_elements']
    index_time_bar = 1
    index_bucket = 1
    bucket_buy_volume = 0
    bucket_sell_volume = 0
    bucket_price = 0
    bucket_num_trades = 0
    volume_count = 0

    data['buy_volumes'] = []
    data['sell_volumes'] = []
    data['bucket_price'] = []
    data['start_time_buckets'] = []
    data['num_trades_in_buckets'] = []
    while (index_time_bar < len_time_bars):
        bar_delta_price = data['price'][index_time_bar]
        if (IS_PRICE_DIFFERENCED == False):
            bar_delta_price = data['price'][index_time_bar] - data['price'][index_time_bar - 1]
        bar_volume = data['volume'][index_time_bar]
        bar_trades = data['num_trades_in_bar'][index_time_bar]
        usable_volume = None
        # If the entire time bar is consumed, go to next bar
        # else subtract the usable volume and get the remaining
        # in the next bucket
        if (bar_volume <= BUCKET_SIZE - volume_count):
            usable_volume = bar_volume
            data['volume'][index_time_bar] -= usable_volume
            index_time_bar += 1
        else:
            usable_volume = (BUCKET_SIZE - volume_count)
            data['volume'][index_time_bar] -= usable_volume

        z_value = norm.cdf(bar_delta_price / data['stdev'])
        buy_volume = int(usable_volume * z_value)
        bucket_buy_volume += buy_volume
        bucket_sell_volume += usable_volume - buy_volume
        bucket_price += bar_delta_price
        bucket_num_trades += bar_trades

        volume_count += usable_volume
        if (volume_count >= BUCKET_SIZE):
            assert volume_count == BUCKET_SIZE, 'volume_count is greater than Bucket Size.'
            assert bucket_buy_volume + bucket_sell_volume == BUCKET_SIZE, 'Volumes do not add up to Bucket Size'
            data['buy_volumes'].append(bucket_buy_volume)
            data['sell_volumes'].append(bucket_sell_volume)
            data['bucket_price'].append(bucket_price)
            data['num_trades_in_buckets'].append(bucket_num_trades)
            data['start_time_buckets'].append(data['start_time'][index_time_bar])
            volume_count = 0
            index_bucket += 1
            bucket_buy_volume = 0
            bucket_sell_volume = 0
            bucket_price = 0
            bucket_num_trades = 0

    data['num_buckets'] = index_bucket - 1
# for ticker, data in aggr_data.iteritems():
#     if 'buy_volumes' in data:
#         print(len(data['buy_volumes']), data['num_buckets'])
#         # pprint (np.add(data['buy_volumes'], data['sell_volumes']))

print('Complete')

## Calculate VPIN
print 'Calculating VPIN: ',
results = {}
time_axis = {}

for ticker, data in aggr_data.iteritems():
    BUCKET_SIZE = None
    if ticker not in BUCKET_SIZE_MAP:
        # BUCKET_SIZE = input('Enter Bucket Size for ' + ticker)
        continue # TODO: Do something with this
    BUCKET_SIZE = BUCKET_SIZE_MAP[ticker]
    total_volume = BUCKET_SIZE * data['num_buckets']

    diff_sell_buy = np.subtract(data['sell_volumes'], data['buy_volumes'])
    abs_value = np.fabs(diff_sell_buy)

    if IS_VPIN_ROLL_FORWARD:
        num_iters = 0
        results[ticker] = []
        time_axis[ticker] = []
        iter_volume = BUCKET_SIZE * BUCKETS_PER_ITER
        while (num_iters + BUCKETS_PER_ITER < data['num_buckets']):
            start_index = num_iters
            end_index = start_index + BUCKETS_PER_ITER
            vpin = np.sum(abs_value[start_index: end_index]) / iter_volume
            results[ticker].append(vpin)
            time_axis[ticker].append(data['start_time_buckets'][end_index])
            num_iters += 1
    else:
        vpin = np.sum(abs_value) / total_volume
        results[ticker] = [vpin]

# print('Complete')
# for ticker in results:
#
#     pprint (results[ticker])
#     pprint (time_axis[ticker])
#     print('Number Bucket: ', aggr_data[ticker]['num_buckets'])
#     pprint(len(aggr_data[ticker]['start_time_buckets']))
# #     pprint(results[ticker])
# #     pprint(time_axis[ticker])
#     data = [go.Scatter(x=time_axis[ticker], y=results[ticker])]
#     plotly.offline.plot(data)

print('Complete')
if VERBOSE:
    print 'Running Verbosity: ',
    verbose_data = [[
        "Ticker",
        "Bucket #",
        "Bucket Start Time",
        "Bucket Start Time Formatted",
        "Bucket Price",
        "Bucket Size",
        "Number Trades",
        "Total Volume",
        "Buy Volume",
        "Sell Volume",
        "StDev",
    ]]
    if IS_VPIN_ROLL_FORWARD:
        verbose_data[0].append("VPIN Roll Forward" + str(BUCKETS_PER_ITER))
    for ticker, data in aggr_data.iteritems():
        for i in range (0, data['num_buckets']):
            datum = [
                ticker,
                i,
                helpers.python_time_to_decimal_time(data['start_time_buckets'][i]),
                data['start_time_buckets'][i],
                data['bucket_price'][i],
                BUCKET_SIZE_MAP[ticker],
                data['num_trades_in_buckets'][i],
                data['buy_volumes'][i] + data['sell_volumes'][i],
                data['buy_volumes'][i],
                data['sell_volumes'][i],
                data['stdev']
            ]
            if IS_VPIN_ROLL_FORWARD:
                if i >= BUCKETS_PER_ITER:
                    datum.append(results[ticker][i - BUCKETS_PER_ITER])
                else:
                    datum.append("")
            verbose_data.append(datum)

    with open(VERBOSE_CSV_FILE_NAME, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerows(verbose_data)

    print('Complete')
