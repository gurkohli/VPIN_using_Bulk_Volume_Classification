from pprint import pprint
from scipy.stats import norm

import json
import sys
import math
import numpy as np

import helpers

LOG_FILE_PATH = None
VERBOSE = None

def config(config):
    global LOG_FILE_PATH
    global VERBOSE

    LOG_FILE_PATH = config['LOG_FILE_PATH']
    VERBOSE = config['VERBOSE']

def verbose_print(text, single_line = False):
    if VERBOSE:
        if single_line:
            print text,
        else:
            print text
        sys.stdout.flush()

def calculate_price_difference(price, num_elements):
    price_diff = []
    # if IS_PRICE_DIFFERENCED:
    #     num_time_bars = num_elements
        # data['price_diff'] = [data['price'][0]]
    for i in range(1, num_elements):
        price_diff.append(price[i] - price[i-1])
            # data['price_diff'].append(data['price'][i] - data['price'][i-1])
        # data['old_price'] = data['price']
        # data['price'] = data['price_diff']

    # else:
    #     if VERBOSE:
    #         print('Skipped')
    return price_diff

def calculate_stdev(price_diff, volume):
    stdev = None

    mean = np.average(price_diff, weights=volume)
    variance = np.average((price_diff-mean)**2, weights=volume)
    stdev = math.sqrt(variance)

    return stdev

def classify_buy_sell_volume(stdev, num_elements, bucket_size, price_orig, volume_orig, num_trades_in_bar, start_time):
    price = price_orig[:]
    volume = volume_orig[:]
    result = {
        'buy_volumes': [],
        'sell_volumes': [],
        'bucket_price': [],
        'start_time_buckets': [],
        'num_trades_in_buckets': [],
    }

    len_time_bars = num_elements
    index_time_bar = 0
    index_bucket = 0
    bucket_buy_volume = 0
    bucket_sell_volume = 0
    bucket_price = 0
    bucket_num_trades = 0
    volume_count = 0

    while (index_time_bar < len_time_bars):
        bar_delta_price = price[index_time_bar]
        bar_volume = volume[index_time_bar]
        bar_trades = num_trades_in_bar[index_time_bar]
        usable_volume = None

        # If the entire time bar is consumed, go to next bar
        # else subtract the usable volume and get the remaining
        # in the next bucket
        if (bar_volume <= bucket_size - volume_count):
            usable_volume = bar_volume
            volume[index_time_bar] -= usable_volume
            index_time_bar += 1
        else:
            usable_volume = (bucket_size - volume_count)
            volume[index_time_bar] -= usable_volume

        z_value = 0.5
        if (stdev != 0):
            z_value = norm.cdf(bar_delta_price / stdev)
        buy_volume = int(usable_volume * z_value)
        bucket_buy_volume += buy_volume
        bucket_sell_volume += usable_volume - buy_volume
        bucket_price += bar_delta_price
        bucket_num_trades += bar_trades

        volume_count += usable_volume
        if (volume_count >= bucket_size):
            assert volume_count == bucket_size, 'volume_count is greater than Bucket Size.'
            assert bucket_buy_volume + bucket_sell_volume == bucket_size, 'Volumes do not add up to Bucket Size'

            result['buy_volumes'].append(bucket_buy_volume)
            result['sell_volumes'].append(bucket_sell_volume)
            result['bucket_price'].append(bucket_price)
            result['num_trades_in_buckets'].append(bucket_num_trades)
            result['start_time_buckets'].append(start_time[index_time_bar])

            volume_count = 0
            index_bucket += 1
            bucket_buy_volume = 0
            bucket_sell_volume = 0
            bucket_price = 0
            bucket_num_trades = 0

    result['num_buckets'] = index_bucket
    return result

def calculate_vpin(num_buckets, bucket_size, sell_volumes, buy_volumes, start_time_buckets):

    total_volume = bucket_size * num_buckets

    diff_sell_buy = np.subtract(sell_volumes, buy_volumes)
    abs_value = np.fabs(diff_sell_buy)

    # if IS_VPIN_ROLL_FORWARD and not IS_ALGORITHM_RUN_IN_INTERVALS:
    #     num_iters = 0
    #     results = []
    #     time_axis= []
    #     iter_volume = bucket_size * BUCKETS_PER_ITER
    #     while (num_iters + BUCKETS_PER_ITER < num_buckets):
    #         start_index = num_iters
    #         end_index = start_index + BUCKETS_PER_ITER
    #         vpin = np.sum(abs_value[start_index: end_index]) / iter_volume
    #         results.append(vpin)
    #         time_axis.append(start_time_buckets[end_index])
    #         num_iters += 1
    # else:
    results = []
    time_axis = []
    if (total_volume != 0):
        vpin = np.sum(abs_value) / total_volume
        results = [vpin]
        time_axis = [start_time_buckets[num_buckets - 1]]
    return (results, time_axis)

def algorithm(data, bucket_size, ticker):
    if data['num_elements'] <= 1:
        return ([], [], [])

    orig_volume = data['volume'][:]
    price_difference = {}
    num_elements = data['num_elements']
    volume = data['volume']
    price = data['price']
    num_trades_in_bar = data['num_trades_in_bar'][1:]
    start_time = data['start_time']

    ## Calculate delta(P), price difference between aggregated time bars
    verbose_print('Calculating Price Difference: ', True)
    price_difference = calculate_price_difference(price, num_elements)
    verbose_print('Complete')


    ## Calculate standard deviation, stdev across aggregates
    volume_new = volume[1:]
    num_elements_new = num_elements - 1


    verbose_print('Calculating Standard Deviation: ', True)
    stdev = calculate_stdev(price_difference, volume_new)
    verbose_print('Complete')

    ## Classify buy / sell volume (loop)
    verbose_print('Classifying Volume: ', True)
    result = classify_buy_sell_volume(stdev, num_elements_new, bucket_size, price_difference, volume_new, num_trades_in_bar, start_time)
    verbose_print('Complete')
    ## Calculate VPIN

    buy_volumes = result['buy_volumes']
    sell_volumes = result['sell_volumes']
    bucket_price = result['bucket_price']
    num_trades_in_buckets = result['num_trades_in_buckets']
    start_time_buckets = result['start_time_buckets']
    num_buckets = result['num_buckets']

    # if (num_buckets == 0):
    #     pprint(stdev)
    #     pprint(num_elements_new)
    #     pprint(bucket_size)
    #     pprint(price_difference)
    #     pprint(volume_new)
    #     pprint(num_trades_in_bar)
    #     pprint(start_time)
    #     pprint(result)
    #     pprint(data)
    verbose_print('Calculating VPIN: ', True)
    (vpin, time_axis) = calculate_vpin(num_buckets, bucket_size, sell_volumes, buy_volumes, start_time_buckets)
    verbose_print('Complete')

    data_dump = {
        'num_buckets': num_buckets,
        'start_time_buckets': start_time_buckets,
        'bucket_price': bucket_price,
        'num_trades_in_buckets': num_trades_in_buckets,
        'buy_volumes': buy_volumes,
        'sell_volumes': sell_volumes,
        'stdev': stdev,
    }
    return (vpin, time_axis, data_dump)
