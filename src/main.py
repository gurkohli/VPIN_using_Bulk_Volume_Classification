from pprint import pprint

import time
import sys
import glob
import os
import datetime as DT
import argparse

import helpers
import algorithm

config = {
    'DATA_DIR_PATH': os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data'),
    'LOG_FILE_PATH': os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'logs.txt'),
    'RESULTS_DIR_PATH': os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'results'),
    'TIME_BAR_SIZE': DT.timedelta(minutes=1), # 1 minute
    'VERBOSE': False,
    'VERBOSE_CSV_FILE_PREFIX': 'OTPT_',
    'VERBOSE_CSV_FILE_SUFFIX': '.out',
    'VOLUME_DIR_PATH': os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'volume'),
    'XA_DATE_LIMIT': DT.date(2010,01,01),
    'XB_DATE_LIMIT': DT.date(2015,07,01),
}

def check_stdev_arg(value):
    if (value != 'average' and value != 'lastval'):
        raise argparse.ArgumentTypeError("--stdev_mode argument should either be average or lastval")
    return value

### -- MAIN function starts here --
algorithm_start_time = time.time()

parser = argparse.ArgumentParser()
parser.add_argument("--stdev_mode", help="Stdev mode (average or lastval)", nargs='?', type=check_stdev_arg, default='average')
args = parser.parse_args()

stdev_mode = args.stdev_mode;
config['IS_STDEV_CALCULATION_AVERAGE'] = True
if (stdev_mode == 'average'):
    print 'Using average value as mean for Standard Deviation calculation'
elif (stdev_mode == 'lastval'):
    print 'Using last timebar value as mean for Standard Deviation calculation'
    config['IS_STDEV_CALCULATION_AVERAGE'] = False

algorithm.config(config)

print 'Getting list of files:',
sys.stdout.flush()
filelist = sorted(glob.glob(os.path.join(config['DATA_DIR_PATH'], '*.zip')))
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
        algorithm.algorithm(start_period, end_period)

algorithm_end_time = time.time()

print ('Algorithm Timing Statistics:')
print ('Start Time: ', str(time.ctime(algorithm_start_time)))
print ('End Time: ', str(time.ctime(algorithm_end_time)))
print ('Time Elapsed: ', algorithm_end_time - algorithm_start_time)
