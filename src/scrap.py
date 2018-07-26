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
TIME_BAR_SIZE =
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
