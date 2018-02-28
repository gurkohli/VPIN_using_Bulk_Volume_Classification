from datetime import date, time, timedelta

# Assuming decimal_time is in seconds
def decimal_time_to_python_time(decimal_time):
    t = float(decimal_time) / 3600
    hours = int(t)

    t = (t - hours) * 60
    minutes = int(t)

    seconds = int((t - minutes) * 60)

    return time(hours, minutes, seconds)


def python_time_to_decimal_time(python_time):
    time = python_time.hour * 3600

    time += python_time.minute * 60

    time += python_time.second

    return time
# Assuming date_str -> YYYYMMDD
def string_date_to_python_date(date_str):
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])

    return date(year, month, day)

# Round a datetime object to a multiple of a timedelta
# https://stackoverflow.com/questions/3463930
# dt : datetime.datetime object, default now.
# dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
# Author: Thierry Husson 2012 - Use it as you want but don't blame me.
#         Stijn Nevens 2014 - Changed to use only datetime objects as variables
#         Gur Kohli 2017 - Changed to always round down
#
def round_time(dt=None, dateDelta=timedelta(minutes=1)):
    roundTo = dateDelta.total_seconds()

    if dt == None : dt = datetime.now()
    seconds = (dt - dt.min).seconds
    # // is a floor division, not a comment on following line:
    rounding = seconds // roundTo * roundTo
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)
