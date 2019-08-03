"""Ip Timeline in PySpark"""

import os
import sys
import argparse

from datetime import datetime
from pyspark import SparkContext

parser = argparse.ArgumentParser(description='Build a time series from IP log files.')

parser.add_argument('--input', nargs=1, required=True,
                   help='input file')
parser.add_argument('--output', nargs=1, required=True,
                   help='output directory')

def row_to_flat_time(row):
    local_time = datetime.strptime(row.partition(",")[0], "%d/%b/%Y %H:%M:%S")
    return (local_time.replace(second=0), 1)

if __name__ == "__main__":

    args = parser.parse_args()
    
    in_file = args.input[0]
    out_file = args.output[0]

    print("Input:", in_file)
    print("Output:", out_file)

    sc = SparkContext(appName="IpTimeline")

    input = sc.textFile(in_file)

    input\
        .filter(lambda x: len(x.strip()) > 0 and not x.startswith("Date/time"))\
        .map(row_to_flat_time)\
        .reduceByKey(lambda l,r: l+r)\
        .sortBy(lambda tup: tup[0])\
        .map(lambda tup: "%s\t%d" % (tup[0].strftime("%s"), tup[1]))\
        .saveAsTextFile(out_file)







