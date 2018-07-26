from pprint import pprint

import datetime as DT
import os
import sys
import math
import zipfile
import time

import helpers
import vpin

XA_DATE_LIMIT = DT.date(2010,01,01)
XB_DATE_LIMIT = DT.date(2015,07,01)

DATA_DIR_PATH = None
IS_STDEV_CALCULATION_AVERAGE = None
LOG_FILE_PATH = None
VOLUME_DIR_PATH = None
VERBOSE_CSV_FILE_PREFIX = None
VERBOSE_CSV_FILE_SUFFIX = None
RESULTS_DIR_PATH = None
TIME_BAR_SIZE = None
XA_DATE_LIMIT = None
XB_DATE_LIMIT = None

def config(config):
    vpin.config(config)

    global DATA_DIR_PATH
    global IS_STDEV_CALCULATION_AVERAGE
    global LOG_FILE_PATH
    global RESULTS_DIR_PATH
    global TIME_BAR_SIZE
    global VOLUME_DIR_PATH
    global VERBOSE_CSV_FILE_PREFIX
    global VERBOSE_CSV_FILE_SUFFIX
    global XA_DATE_LIMIT
    global XB_DATE_LIMIT

    DATA_DIR_PATH = config['DATA_DIR_PATH']
    IS_STDEV_CALCULATION_AVERAGE = config['IS_STDEV_CALCULATION_AVERAGE']
    LOG_FILE_PATH = config['LOG_FILE_PATH']
    RESULTS_DIR_PATH = config['RESULTS_DIR_PATH']
    VOLUME_DIR_PATH = config['VOLUME_DIR_PATH']
    VERBOSE_CSV_FILE_PREFIX = config['VERBOSE_CSV_FILE_PREFIX']
    VERBOSE_CSV_FILE_SUFFIX = config['VERBOSE_CSV_FILE_SUFFIX']
    TIME_BAR_SIZE = config['TIME_BAR_SIZE']
    XA_DATE_LIMIT = config['XA_DATE_LIMIT']
    XB_DATE_LIMIT = config['XB_DATE_LIMIT']


def build_bucket_size_map(filename):
    print 'Building bucket_size_map from file ' + filename + ' :',
    bucket_size_map = {}
    sys.stdout.flush()
    with open(os.path.join(VOLUME_DIR_PATH, filename)) as file:
        for line in file:
            if 'str' in line:
                break
            ticker = line[0:9].strip()
            mdv = float(line[131:145].strip()) #TODO: Check if this exists
            bucket_size = int(math.ceil(mdv / 50))
            bucket_size_map[ticker] = bucket_size
    print(' Complete')
    return bucket_size_map

def get_data_filename(current_date):
    classifier = 'XC'
    if (current_date < XA_DATE_LIMIT):
        classifier = 'XA'
    elif (current_date < XB_DATE_LIMIT):
        classifier = 'XB'

    data_filename = 'Trade by Trade_' + classifier
    data_filename += '_' + current_date.strftime('%Y%m%d') + '.zip'
    data_filepath = os.path.join(DATA_DIR_PATH, data_filename)
    return data_filepath

def unzip_file(data_filepath):
    sys.stdout.flush()
    zip_ref = zipfile.ZipFile(data_filepath, 'r')
    files_in_zip = zip_ref.namelist()
    assert len(files_in_zip) == 1 , 'Zip file contains more than one filename'
    file = zip_ref.open(files_in_zip[0], 'r')
    zip_ref.close()
    return file

def parse_data(file, current_date):
    data = {}
    time_index = time.time()
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
    return data

def aggregate_data(transactions):
    aggr_data = {
        'num_elements': 0,
        'price': [],
        'volume': [],
        'start_time': [], # TODO - Remove this if we don't need it
        'num_trades_in_bar': [],
    }
    len_transactions = len(transactions)
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
            aggr_data['num_elements'] += 1
            aggr_data['start_time'].append(start_time)
            aggr_data['price'].append(price)
            aggr_data['volume'].append(volume)
            aggr_data['num_trades_in_bar'].append(num_trades_in_bar)
            price = 0.0
            volume = 0
            last_price = 0
            num_trades_in_bar = 0
            start_time += TIME_BAR_SIZE
    return aggr_data

def process_data(bucket_size, full_data, ticker):
    data_dump = {}

    (results_i, time_axis_i, data_dump_i) = vpin.algorithm(full_data, bucket_size, ticker)
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

    # results[ticker].extend(results_i)
    # time_axis[ticker].extend(time_axis_i)
    # data_dump = helpers.extend_dict_of_arrays(data_dump, data_dump_i)

    return (results_i, time_axis_i, data_dump_i)

def writeOutputToFile(interim_data, result_date_str, bucket_size_map):
    filename = VERBOSE_CSV_FILE_PREFIX + result_date_str + VERBOSE_CSV_FILE_SUFFIX
    print 'Outputting data for date ' + result_date_str + ' to file ' + filename + ':',
    sys.stdout.flush()
    # Bucket Data
    if not interim_data:
        open(os.path.join(RESULTS_DIR_PATH, filename), "w").close()
        print('Complete')
        return

    verbose_data = []
    for ticker, data in interim_data.iteritems():
        if 'num_buckets' not in data:
            continue
        if ticker not in bucket_size_map:
            print('Ticker not found in bucket_size_map')
            continue
        for i in range (0, data['num_buckets']):
            datum = [
                ticker,
                helpers.python_date_to_string_date(data['start_time_buckets'][i]),
                helpers.python_time_to_decimal_time(data['start_time_buckets'][i]),
                data['bucket_price'][i],
                i,
                bucket_size_map[ticker],
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
    bucket_size_map = {}

    delta_period = end_period - start_period
    for date_index in range(delta_period.days+1):
        aggr_data = {}
        file = None
        data_dump = {}
        parsed_data = {}
        results = {}
        time_axis = {}

        current_date = start_period + DT.timedelta(days=date_index)
        data_filepath = get_data_filename(current_date)
        data_filename = os.path.basename(data_filepath)

        if (not os.path.exists(data_filepath)):
            # Write blank file
            current_date_str = current_date.strftime('%Y%m%d');
            print data_filepath + ' does not exist. Writing blank file'
            writeOutputToFile({}, current_date_str, bucket_size_map)
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
                        writeOutputToFile({}, date_str, bucket_size_map)

            vol_file_date = current_date.replace(day=1)
            vol_file_date_str = current_date.strftime('%Y%m')
            vol_filename = 'VPIN1001_' + vol_file_date_str + '.out'
            bucket_size_map = build_bucket_size_map(vol_filename)

        print 'Unzipping file ' + data_filename + ': ',
        file = unzip_file(data_filepath)
        print(' Complete')

        print 'Parsing data from file ' + data_filename + ': ',
        sys.stdout.flush()
        try :
            parsed_data = parse_data(file, current_date)
        except IOError:
            print(' Error!!!!')
            print 'Error occured while parsing file. Logging filename and continuing to next file'
            current_date_str = current_date.strftime('%Y%m%d');
            writeOutputToFile({}, current_date_str, bucket_size_map)
            with open(LOG_FILE_PATH, "a") as myfile:
                myfile.write(data_filename + '\n')
            continue
        print('Complete')

        print 'Aggregating Data: ',
        sys.stdout.flush()
        for ticker in parsed_data:
            if (len(parsed_data[ticker]) == 0):
                continue;
            aggr_data[ticker] = aggregate_data(parsed_data[ticker])
        print('Complete')

        parsed_data = {}; # Clear data variable to remove memory allocation

        print 'Processing Data: ',
        sys.stdout.flush()
        time_index = time.time()
        for ticker, full_data in aggr_data.iteritems():
            if ticker not in bucket_size_map:
                continue
            if (time.time() > time_index + 1):
                print '.',
                sys.stdout.flush()
                time_index = time.time()
            bucket_size = bucket_size_map[ticker]
            (results_i, time_axis_i, data_dump_i) = process_data(bucket_size, full_data, ticker)

            results[ticker] = results_i
            time_axis[ticker] = time_axis_i
            data_dump[ticker] = data_dump_i

            # results[ticker].extend(results_i)
            # time_axis[ticker].extend(time_axis_i)
            # data_dump = helpers.extend_dict_of_arrays(data_dump, data_dump_i)
        print ('Complete')

        # with open(LOG_FILE_PATH, "a") as myfile:
        #     myfile.write(json.dumps(aggr_data, indent=4, sort_keys=True, default=str))

        # Write output to file
        current_date_str = current_date.strftime('%Y%m%d');
        writeOutputToFile(data_dump, current_date_str, bucket_size_map)
