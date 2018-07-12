## Imports
from pprint import pprint
from scipy.stats import norm
import argparse
import sys
import plotly
import plotly.graph_objs as go
import datetime as DT
import numpy as np
import math
import csv
import zipfile
import os
import glob
import time
import json

import helpers

## Constants
DATA_DIR_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
VOLUME_DIR_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'volume')
RESULTS_DIR_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'results')
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logger.txt')
BUCKET_SIZE_FILE_NAME = 'VPIN1001_201201.out'
FILE_NAME = '20120103.xc'
IS_DATA_AGGREGATED = True
IS_PRICE_DIFFERENCED = True
IS_VPIN_ROLL_FORWARD = False
IS_ALGORITHM_RUN_IN_INTERVALS = False

ALGORITHM_INTERVAL_SIZE = DT.timedelta(days=1)
TIME_BAR_SIZE = DT.timedelta(minutes=1) # 1 minute
BUCKETS_PER_ITER = 50
XA_DATE_LIMIT = DT.date(2010,01,01)
XB_DATE_LIMIT = DT.date(2015,07,01)

VERBOSE = False;
VERBOSE_CSV_FILE_PREFIX = "OTPT_"
VERBOSE_CSV_FILE_SUFFIX = ".out"

## Arguments Parsing
def check_stdev_arg(value):
    if (value != 'average' and value != 'lastval'):
        raise argparse.ArgumentTypeError("--stdev_mode argument should either be average or lastval")
    return value

parser = argparse.ArgumentParser()
parser.add_argument("--stdev_mode", help="Stdev mode (average or lastval)", nargs='?', type=check_stdev_arg, default='average')
args = parser.parse_args()

stdev_mode = args.stdev_mode;
IS_STDEV_CALCULATION_AVERAGE = True
if (stdev_mode == 'average'):
    print 'Using average value as mean for Standard Deviation calculation'
elif (stdev_mode == 'lastval'):
    print 'Using last timebar value as mean for Standard Deviation calculation'
    IS_STDEV_CALCULATION_AVERAGE = False
## Function definitions
def VPIN_algorithm(data, BUCKET_SIZE, ticker):
    if (ticker == 'A'):
        print 'Logging DATA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
        with open(LOG_FILE_PATH, "a") as myfile:
            myfile.write(json.dumps(data, indent=4, sort_keys=True, default=str))

    ## TODO Delete this when done with it
    data['orig_volume'] = data['volume'][:]

    if data['num_elements'] <= 1:
        return ([], [], [])
    ## Calculate delta(P), price difference between aggregated time bars
    if VERBOSE:
        print 'Calculating Price Difference: ',
    if IS_PRICE_DIFFERENCED:
        num_time_bars = data['num_elements']
        data['price_diff'] = [data['price'][0]]
        for i in range(1, num_time_bars):
            data['price_diff'].append(data['price'][i] - data['price'][i-1])
        data['old_price'] = data['price']
        data['price'] = data['price_diff']
        if VERBOSE:
            print('Complete')
    else:
        if VERBOSE:
            print('Skipped')

    # pprint(data['price'])
    ## Calculate standard deviation, stdev across aggregates
    if VERBOSE:
        print 'Calculating Standard Deviation: ',
    sys.stdout.flush()
    # for ticker, data in aggr_data.iteritems():

    mean = np.average(data['price'], weights=data['volume'])
    variance = np.average((data['price']-mean)**2, weights=data['volume'])
    data['stdev'] = math.sqrt(variance)

    if VERBOSE:
        print('Complete')

    ## Expand no of observations

    ## Classify buy / sell volume (loop)
    if VERBOSE:
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

        z_value = 0.5
        if (data['stdev'] != 0):
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

    if VERBOSE:
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

    data_dump = {
        'num_buckets': data['num_buckets'],
        'start_time_buckets': data['start_time_buckets'],
        'bucket_price': data['bucket_price'],
        'num_trades_in_buckets': data['num_trades_in_buckets'],
        'buy_volumes': data['buy_volumes'],
        'sell_volumes': data['sell_volumes'],
        'stdev': data['stdev'],
    }
    if VERBOSE:
        print('Complete')
    return (results, time_axis, data_dump)

def build_bucket_size_map(filename):
    print 'Building BUCKET_SIZE_MAP from file ' + filename + ' :',
    BUCKET_SIZE_MAP = {}
    sys.stdout.flush()
    with open(os.path.join(VOLUME_DIR_PATH, filename)) as file:
        for line in file:
            if 'str' in line:
                break
            ticker = line[0:9].strip()
            mdv = float(line[131:145].strip()) #TODO: Check if this exists
            bucket_size = int(math.ceil(mdv / 50))
            BUCKET_SIZE_MAP[ticker] = bucket_size
    print(' Complete')
    return BUCKET_SIZE_MAP

def writeOutputToFile(interim_data, result_date_str, BUCKET_SIZE_MAP):
    filename = VERBOSE_CSV_FILE_PREFIX + result_date_str + VERBOSE_CSV_FILE_SUFFIX
    print 'Outputting data for date ' + result_date_str + ' to file ' + filename + ':',
    sys.stdout.flush()
    # Bucket Data

    verbose_data = []
    for ticker, data in interim_data.iteritems():
        if 'num_buckets' not in data:
            continue
        if ticker not in BUCKET_SIZE_MAP:
            print('Ticker not found in BUCKET_SIZE_MAP')
            continue
        for i in range (0, data['num_buckets']):
            datum = [
                ticker,
                helpers.python_date_to_string_date(data['start_time_buckets'][i]),
                helpers.python_time_to_decimal_time(data['start_time_buckets'][i]),
                data['bucket_price'][i],
                i,
                BUCKET_SIZE_MAP[ticker],
                data['num_trades_in_buckets'][i],
                data['buy_volumes'][i] + data['sell_volumes'][i],
                data['buy_volumes'][i],
                data['sell_volumes'][i],
                data['stdev_list'][i],
                data['vpin_list'][i],
            ]
            verbose_data.append(datum)

    # /*    TICKER         A9,1X       1-9                              */
    # /*    DATE           I10         11-20                            */
    # /*    TIME           F15.6,5X    21-40                            */
    # /*    PRICE          F15.4,5X    41-60                            */
    # /*    BUCKNUM        I10         61-70                            */
    # /*    BUCKSIZE       I10         71-80                            */
    # /*    NTRADE         I10         81-90                            */
    # /*    TOTVOL         I10         91-100                           */
    # /*    BUYVOL         E15.8,5X    101-120                          */
    # /*    SELLVOL        E15.8,5X    121-140                          */
    # /*    STDEV          E15.8,5X    141-160                          */
    # /*    DAYVPIN        E15.8,5X    161-180                          */
    delimeter_spaces = [10, 10, 20, 20, 10, 10, 10, 10, 20, 20, 20, 20]
    with open(os.path.join(RESULTS_DIR_PATH, filename), "w") as otpt:
        for row in verbose_data:
            for index, column in enumerate(row):
                space = '%-' + str(delimeter_spaces[index]) + 's'
                otpt.write(space % (column))
            otpt.write('\n')

    print('Complete')

def algorithm(start_period, end_period):
    vol_file_date = None
    BUCKET_SIZE_MAP = {}

    delta_period = end_period - start_period
    for date_index in range(delta_period.days+1):
        results = {}
        time_axis = {}
        data = {}
        data_dump = {}

        current_date = start_period + DT.timedelta(days=date_index)
        classifier = 'XC'
        if (current_date < XA_DATE_LIMIT):
            classifier = 'XA'
        elif (current_date < XB_DATE_LIMIT):
            classifier = 'XB'

        data_filename = 'Trade by Trade_' + classifier
        data_filename += '_' + current_date.strftime('%Y%m%d') + '.zip'
        data_filepath = os.path.join(DATA_DIR_PATH, data_filename)

        if (not os.path.exists(data_filepath)):
            # Write blank file
            current_date_str = current_date.strftime('%Y%m%d');
            print data_filepath + ' does not exist. Writing blank file'
            writeOutputToFile(data_dump, current_date_str, BUCKET_SIZE_MAP)
            continue

        if (vol_file_date == None or
                (vol_file_date.year != current_date.year
                    or vol_file_date.month != current_date.month
            )):
            if (vol_file_date != None):
                days_in_prev_month = helpers.get_num_of_days_for_date(vol_file_date)
                # Output invalid dates to file as well so every month has 31 filelist
                if (days_in_prev_month < 31):
                    for day in range(days_in_prev_month+1, 31+1):
                        date_str = vol_file_date.strftime('%Y%m') + str(day)
                        writeOutputToFile({}, date_str, BUCKET_SIZE_MAP)

            vol_file_date = current_date.replace(day=1)
            vol_file_date_str = current_date.strftime('%Y%m')
            vol_filename = 'VPIN1001_' + vol_file_date_str + '.out'
            BUCKET_SIZE_MAP = build_bucket_size_map(vol_filename)

        print 'Unzipping file ' + data_filename + ': ',
        sys.stdout.flush()
        zip_ref = zipfile.ZipFile(data_filepath, 'r')
        files_in_zip = zip_ref.namelist()
        assert len(files_in_zip) == 1 , 'Zip file contains more than one filename'
        file = zip_ref.open(files_in_zip[0], 'r')
        zip_ref.close()
        print(' Complete')

        time_index = time.time()
        print 'Parsing data from file ' + data_filename + ': ',
        sys.stdout.flush()

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
            if (current_date < XA_DATE_LIMIT):
                date_parsed = helpers.string_date_to_python_date(line[12:20].strip())
                time_parsed = helpers.decimal_time_to_python_time(line[20:30].strip())
            elif (current_date < XB_DATE_LIMIT):
                date_parsed = helpers.string_date_to_python_date(line[18:26].strip())
                time_parsed = helpers.decimal_time_to_python_time(line[26:36].strip())
            else:
                date_parsed = helpers.string_date_to_python_date(line[18:26].strip())
                time_parsed = helpers.decimal_time_to_python_time(line[26:41].strip())
            parsed_dt = DT.datetime.combine(date_parsed, time_parsed)

            price = float(line[50:70].strip())
            volume = int(line[70:90].strip())
            if (volume < 0 or price < 0):
                continue
            data[ticker].append([parsed_dt, price, volume])
        print('Complete')

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
                if (len_transactions <= 0):
                    continue;
                index = 1
                price = transactions[0][1]
                volume = transactions[0][2]
                start_time = helpers.round_time(transactions[0][0], TIME_BAR_SIZE)
                num_trades_in_bar = 1;
                last_price = price
                while (index < len_transactions):
                    transaction = transactions[index]
                    if transaction[0] <= start_time + TIME_BAR_SIZE:
                        price += transaction[1]
                        last_price = transaction[1]
                        volume += transaction[2]
                        num_trades_in_bar += 1;
                        index += 1
                    else:
                        if (IS_STDEV_CALCULATION_AVERAGE):
                            if num_trades_in_bar != 0:
                                price = price / num_trades_in_bar
                        else:
                            price = last_price
                        if (price == 0):
                            price = 0.0
                            volume = 0
                            num_trades_in_bar = 0
                            last_price = 0
                            start_time += TIME_BAR_SIZE
                            continue
                        aggr_data[ticker]['num_elements'] += 1
                        aggr_data[ticker]['start_time'].append(start_time)
                        aggr_data[ticker]['price'].append(price)
                        aggr_data[ticker]['volume'].append(volume)
                        aggr_data[ticker]['num_trades_in_bar'].append(num_trades_in_bar)
                        price = 0.0
                        volume = 0
                        last_price = 0
                        num_trades_in_bar = 0
                        start_time += TIME_BAR_SIZE
            print('Complete')

        print 'Processing Data: ',
        time_index = time.time()
        sys.stdout.flush()
        data = {}; # Clear data variable to remove memory allocation
        for ticker, full_data in aggr_data.iteritems():
            if (IS_VPIN_ROLL_FORWARD and IS_ALGORITHM_RUN_IN_INTERVALS):
                print('     Warning: VPIN Roll Forward is requested but algorithm is running in Interval Mode. Roll forward will be ignored')
            if ticker not in BUCKET_SIZE_MAP:
                continue # TODO: Do something with this
            BUCKET_SIZE = BUCKET_SIZE_MAP[ticker]

            if (ticker not in results):
                results[ticker] = []
                time_axis[ticker] = []
                data_dump[ticker] = {}

            (results_i, time_axis_i, data_dump_i) = VPIN_algorithm(full_data, BUCKET_SIZE, ticker)
            if (len(results_i) == 0):
                data_dump_i = {
                    'num_buckets': 0,
                    'stdev': 0,
                    'stdev_list': [],
                    'vpin_list': [],
                    'start_time_buckets': [],
                    'bucket_price': [],
                    'num_trades_in_buckets': [],
                    'buy_volumes': [],
                    'sell_volumes': [],
                }
            else:
                num_new_buckets = data_dump_i['num_buckets']
                new_stdev_list = [data_dump_i['stdev']]*(num_new_buckets)
                new_vpin_list = [results_i[0]]*(num_new_buckets)
                data_dump_i['stdev_list'] = new_stdev_list
                data_dump_i['vpin_list'] = new_vpin_list

            results[ticker].extend(results_i)
            time_axis[ticker].extend(time_axis_i)
            if ticker not in data_dump:
                pprint(data_dump)
            data_dump[ticker] = helpers.extend_dict_of_arrays(data_dump[ticker], data_dump_i)
            if (time.time() > time_index + 1):
                print '.',
                sys.stdout.flush()
                time_index = time.time()

        print ('Complete')
        # Write output to file
        current_date_str = current_date.strftime('%Y%m%d');
        # with open(LOG_FILE_PATH, "a") as myfile:
        #     myfile.write(json.dumps(aggr_data, indent=4, sort_keys=True, default=str))
        writeOutputToFile(data_dump, current_date_str, BUCKET_SIZE_MAP)

### -- MAIN function starts here --
algorithm_start_time = time.time()

print 'Getting list of files:',
sys.stdout.flush()
filelist = sorted(glob.glob(os.path.join(DATA_DIR_PATH, '*.zip')))
print(' Complete')

len_filelist = len(filelist)

if (len_filelist == 0):
    print ('No files found')
else:
    print ('Found ' + str(len_filelist) + ' files')
    start_index = 0;
    # Calculate Start Period
    start_period = None;
    end_period = None;
    while(start_period == None or start_period == -1):
        if start_index >= len(filelist):
            break
        start_file = os.path.basename(filelist[start_index])
        start_period_str = start_file.split('.')[0][-8:]
        start_period = helpers.string_date_to_python_date(start_period_str)
        start_index += 1;

    end_index = len(filelist)-1;
    while(end_period == None or end_period == -1):
        if end_index < 0:
            break
        end_file = os.path.basename(filelist[end_index])
        end_period_str = end_file.split('.')[0][-8:]
        end_period = helpers.string_date_to_python_date(end_period_str)
        end_index -= 1;

    if ((start_index >= len(filelist) or end_index < 0) and len_filelist > 1):
        print 'Could not find a valid start or end period. Quitting'
    else:
        if (len_filelist == 1):
            end_period = start_period + DT.timedelta(days=1)
        algorithm(start_period, end_period)

algorithm_end_time = time.time()

print ('Algorithm Timing Statistics:')
print ('Start Time: ', str(time.ctime(algorithm_start_time)))
print ('End Time: ', str(time.ctime(algorithm_end_time)))
print ('Time Elapsed: ', algorithm_end_time - algorithm_start_time)
