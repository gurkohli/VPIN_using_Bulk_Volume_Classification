## Imports
from pprint import pprint
from scipy.stats import norm
import sys
import plotly
import plotly.graph_objs as go
import datetime as DT
import numpy as np
import math
import csv
import zipfile
import os
import time

import helpers

## Constants
DATA_DIR_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
RESULTS_DIR_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'results')
BUCKET_SIZE_FILE_NAME = 'VPIN1001_201201.out'
FILE_NAME = '20120103.xc'
IS_DATA_AGGREGATED = True
IS_PRICE_DIFFERENCED = True
IS_VPIN_ROLL_FORWARD = True
IS_ALGORITHM_RUN_IN_INTERVALS = True

ALGORITHM_INTERVAL_SIZE = DT.timedelta(days=1)
TIME_BAR_SIZE = DT.timedelta(minutes=1) # 1 minute
BUCKET_SIZE_MAP = {
}
BUCKETS_PER_ITER = 50

VERBOSE = True;
VERBOSE_CSV_FILE_PREFIX = "verbose_data"
VERBOSE_CSV_FILE_SUFFIX = ".csv"

def VPIN_algorithm(data, BUCKET_SIZE):

    ## TODO Delete this when done with it
    data['orig_volume'] = data['volume'][:]

    if data['num_elements'] <= 1:
        return ([], [], [])
    ## Calculate delta(P), price difference between aggregated time bars
    print 'Calculating Price Difference: ',
    if IS_PRICE_DIFFERENCED:
        num_time_bars = data['num_elements']
        data['price_diff'] = [data['price'][0]]
        for i in range(1, num_time_bars):
            data['price_diff'].append(data['price'][i] - data['price'][i-1])
        data['old_price'] = data['price']
        data['price'] = data['price_diff']
        print('Complete')
    else:
        print('Skipped')

    #pprint(data['price'])
    ## Calculate standard deviation, stdev across aggregates
    print 'Calculating Standard Deviation: ',
    sys.stdout.flush()
    # for ticker, data in aggr_data.iteritems():
    # Volume weighted average
    average = np.average(data['price'], weights=data['volume'])
    variance = np.average((data['price']-average)**2, weights=data['volume'])
    data['stdev'] = math.sqrt(variance)

    # print(data['stdev'])
    print('Complete')
    #pprint(aggr_data['A'])

    ## Expand no of observations

    ## Classify buy / sell volume (loop)
    print 'Classifying Volume: ',
    sys.stdout.flush()
    # for ticker, data in aggr_data.iteritems():

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

    #pprint(data)
    #print('Bucket Size = ', BUCKET_SIZE)
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
            data['start_time_buckets'].append(data['start_time'][index_time_bar - 1])
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
    sys.stdout.flush()

    results = {}
    time_axis = {}

    total_volume = BUCKET_SIZE * data['num_buckets']

    diff_sell_buy = np.subtract(data['sell_volumes'], data['buy_volumes'])
    abs_value = np.fabs(diff_sell_buy)

    if IS_VPIN_ROLL_FORWARD and not IS_ALGORITHM_RUN_IN_INTERVALS:
        num_iters = 0
        results = []
        time_axis= []
        iter_volume = BUCKET_SIZE * BUCKETS_PER_ITER
        while (num_iters + BUCKETS_PER_ITER < data['num_buckets']):
            start_index = num_iters
            end_index = start_index + BUCKETS_PER_ITER
            vpin = np.sum(abs_value[start_index: end_index]) / iter_volume
            results.append(vpin)
            time_axis.append(data['start_time_buckets'][end_index])
            num_iters += 1
    else:
        # print('Num buckets: ', data['num_buckets'])
        # print('Length start_time_buckets', len(data['start_time_buckets']))
        if (data['num_buckets'] == 0):
            results = []
        else:
            vpin = np.sum(abs_value) / total_volume
            results = [vpin]
            time_axis= [data['start_time_buckets'][data['num_buckets'] - 1]]

    print('Complete')
    return (results, time_axis, data)




### -- MAIN function starts here --
algorithm_start_time = time.time()

print 'Building BUCKET_SIZE_MAP:',
sys.stdout.flush()
with open(BUCKET_SIZE_FILE_NAME) as file:
    for line in file:
        if 'str' in line:
            break
        ticker = line[0:9].strip()
        mdv = float(line[131:145].strip()) #TODO: Check if this exists
        bucket_size = int(math.ceil(mdv / 50))
        BUCKET_SIZE_MAP[ticker] = bucket_size

print(' Complete')

print 'Getting list of files:',
sys.stdout.flush()
filelist = os.listdir(DATA_DIR_PATH)
print(' Complete')

print ('Found ' + str(len(filelist)) + ' files')

data = {}
for superfile in filelist:
    if (superfile == '.DS_Store'):
        continue
    print 'Unzipping file ' + superfile + ': ',
    sys.stdout.flush()
    zip_ref = zipfile.ZipFile(os.path.join(DATA_DIR_PATH, superfile), 'r')
    # zip_ref.extractall(DATA_DIR_PATH)
    files_in_zip = zip_ref.namelist()
    assert len(files_in_zip) == 1 , 'Zip file contains more than one filename'
    file = zip_ref.open(files_in_zip[0], 'r')
    zip_ref.close()
    print(' Complete')

    time_index = time.time()
    print 'Parsing data from file ' + superfile + ': ',
    sys.stdout.flush()
    # with open(os.path.join(DATA_DIR_PATH, superfile)) as file:
    for line in file:
        if 'str' in line:
            break
        if (time.time() > time_index + 1):
            print '.',
            sys.stdout.flush()
            time_index = time.time()
        ticker = line[0:9].strip()
        if data.get(ticker) == None:
            data[ticker] = []

        # Parse date time. Assuming date -> YYYYMMDD, time -> decimal(seconds)
        date_parsed = helpers.string_date_to_python_date(line[18:26].strip())
        time_parsed = helpers.decimal_time_to_python_time(line[26:36].strip())
        parsed_dt = DT.datetime.combine(date_parsed, time_parsed)

        price = float(line[50:70].strip())
        volume = int(line[70:90].strip())

        data[ticker].append([parsed_dt, price, volume])
    print('Complete')
    # if (IS_DELETE_UNZIPPED_FILE):
    #     print 'Deleting file ' + superfile + ': ',
    #     os.remove(os.path.join(DATA_DIR_PATH, superfile))
    #     print ('Complete')

# pprint(data)

aggr_data = {}
print 'Aggregating Data: ',
sys.stdout.flush()
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
        start_time = helpers.round_time(transactions[0][0], TIME_BAR_SIZE)
        num_trades_in_bar = 1;
        while (index < len_transactions):
            transaction = transactions[index]
            if transaction[0] <= start_time + TIME_BAR_SIZE:
                price += transaction[1]
                volume += transaction[2]
                num_trades_in_bar += 1;
                index += 1
            else:
                # TODO: Testing average price instead of actual price
                # May need to remove it later
                if num_trades_in_bar != 0:
                    price = price / num_trades_in_bar

                if (price == 0):
                    price = 0.0
                    volume = 0
                    num_trades_in_bar = 0
                    start_time += TIME_BAR_SIZE
                    continue
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

results = {}
time_axis = {}
data_dump = {}

for ticker, full_data in aggr_data.iteritems():
    if (IS_VPIN_ROLL_FORWARD and IS_ALGORITHM_RUN_IN_INTERVALS):
        print('     Warning: VPIN Roll Forward is requested but algorithm is running in Interval Mode. Roll forward will be ignored')
    if ticker not in BUCKET_SIZE_MAP:
        # continue # TODO: Do something with this
        size = input('Enter Bucket Size for ' + ticker + ': ')
        while size <= 0:
            size = input('Invalid Size! Enter Bucket Size (>0) for ' + ticker + ': ')
        BUCKET_SIZE_MAP[ticker] = size
    BUCKET_SIZE = BUCKET_SIZE_MAP[ticker]
    data_dump[ticker] = {
    }
    if (IS_ALGORITHM_RUN_IN_INTERVALS):
        print('Algorithm running in intervals of ' + str(ALGORITHM_INTERVAL_SIZE))
        total_bars = full_data['num_elements']
        start_index = 0
        results[ticker] = []
        time_axis[ticker] = []
        while (start_index < total_bars):
            interim_data = {
                'num_elements': 0,
                'price': [],
                'volume': [],
                'start_time': [],
                'num_trades_in_bar': [],
            }
            initial_start_time = full_data['start_time'][start_index]
            start_time = initial_start_time
            end_index = start_index
            end_time = start_time
            while(end_time < start_time + ALGORITHM_INTERVAL_SIZE and end_index + 1 < total_bars):
                end_index += 1
                end_time = full_data['start_time'][end_index]

            if (start_index == end_index):
                break
            interim_data['num_elements'] = end_index - start_index
            interim_data['price'] = full_data['price'][start_index: end_index]
            interim_data['volume'] = full_data['volume'][start_index: end_index]
            interim_data['start_time'] = full_data['start_time'][start_index: end_index]
            interim_data['num_trades_in_bar'] = full_data['num_trades_in_bar'][start_index: end_index]

            # print(start_index, start_time, end_index, end_time, total_bars)
            (results_i, time_axis_i, data_dump_i) = VPIN_algorithm(interim_data, BUCKET_SIZE)
            if (len(results_i) == 0):
                start_index = end_index
                continue

            results[ticker].extend(results_i)
            time_axis[ticker].extend(time_axis_i)

            ## For verbose analysis
            num_new_buckets = data_dump_i['num_buckets']
            new_stdev_list = [data_dump_i['stdev']]*(num_new_buckets)
            new_vpin_list = [results_i[0]]*(num_new_buckets)
            data_dump_i['stdev_list'] = new_stdev_list
            data_dump_i['vpin_list'] = new_vpin_list

            data_dump[ticker] = helpers.extend_dict_of_arrays(data_dump[ticker], data_dump_i)

            #print(start_time, end_time)
            start_index = end_index
    else:
        (results[ticker], time_axis[ticker], data_dump[ticker]) = VPIN_algorithm(full_data, BUCKET_SIZE)

### -- Results --
# print 'Generating Plots: ',
# sys.stdout.flush()
# traces = []
# for ticker in results:
#
#     # pprint (results[ticker])
#     # pprint (time_axis[ticker])
#     # print('Number Bucket: ', aggr_data[ticker]['num_buckets'])
#     # pprint(len(aggr_data[ticker]['start_time_buckets']))
# #     pprint(results[ticker])
# #     pprint(time_axis[ticker])
#     data = go.Scatter(x=time_axis[ticker], y=results[ticker], name=ticker)
#     if (len(traces) == 0):
#         traces = [data]
#     else:
#         traces.append(data)
# plotly.offline.plot(traces)
#
# print('Complete')



if VERBOSE:
    print 'Running Verbosity: ',
    sys.stdout.flush()
    # Time Bars Data
    verbose_data = [[
        "Ticker",
        "Date",
        "Time Bar #",
        "Price (Last Bar)",
        "Price (Current Bar)",
        "Delta Price",
        "Volume",
    ]]

    for ticker, data in data_dump.iteritems():
        # pprint(data)
        if not data:
            continue
        for i in range (1, data['num_elements']):
            # print (data['old_price'][i]-data['old_price'][i-1], data['price'][i])
            #print data['orig_volume'][i]
            if data['old_price'][i]-data['old_price'][i-1] != data['price'][i]:
                continue
            datum = [
                ticker,
                data['start_time'][i],
                i,
                data['old_price'][i-1],
                data['old_price'][i],
                data_dump[ticker]['price'][i],
                data['orig_volume'][i]
            ]

            verbose_data.append(datum)

    with open(os.path.join(RESULTS_DIR_PATH, VERBOSE_CSV_FILE_PREFIX + '_time_bar_data' + VERBOSE_CSV_FILE_SUFFIX), "w") as otpt:
        writer = csv.writer(otpt, lineterminator='\n')
        writer.writerows(verbose_data)

    # Bucket Data
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
    if IS_VPIN_ROLL_FORWARD and not IS_ALGORITHM_RUN_IN_INTERVALS :
        verbose_data[0].append("VPIN Roll Forward" + str(BUCKETS_PER_ITER))
    else :
        verbose_data[0].append("Day VPIN")
    for ticker, data in data_dump.iteritems():
        if not data:
            continue
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
                data['stdev_list'][i],
            ]
            if IS_VPIN_ROLL_FORWARD and not IS_ALGORITHM_RUN_IN_INTERVALS:
                if i >= BUCKETS_PER_ITER:
                    datum.append(results[ticker][i - BUCKETS_PER_ITER])
                else:
                    datum.append("")
            else:
                datum.append(data['vpin_list'][i])
            verbose_data.append(datum)

    with open(os.path.join(RESULTS_DIR_PATH, VERBOSE_CSV_FILE_PREFIX + '_bucket_data' + VERBOSE_CSV_FILE_SUFFIX), "w") as otpt:
        writer = csv.writer(otpt, lineterminator='\n')
        writer.writerows(verbose_data)

    print('Complete')

algorithm_end_time = time.time()

print ('Algorithm Timing Statistics:')
print ('Start Time: ', str(time.ctime(algorithm_start_time)))
print ('End Time: ', str(time.ctime(algorithm_end_time)))
print ('Time Elapsed: ', algorithm_end_time - algorithm_start_time)
