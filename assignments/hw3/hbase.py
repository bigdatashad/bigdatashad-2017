#!/usr/bin/env python
# vim:set et ts=4 sw=4:

import argparse
import calendar
import getpass
import happybase
import logging
import random
import sys

USAGE = """

To upload sample data (for 2014) to the cluster, run:
  $ {0} --action generate --year 2014

To query daily data for a year, run:
  $ {0} --action query --year 2014

To query daily data for a particular month, run:
  $ {0} --action query --year 2014 --month 10

To query daily data for a particular day, run:
  $ {0} --action query --year 2014 --month 10 --day 27

To compute totals add `--total` argument.

""".format(sys.argv[0])

logging.basicConfig(level="DEBUG")

HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
TABLE = "VisitCountPy-" + getpass.getuser()

def connect():
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)

    logging.debug("Connecting to HBase Thrift Server on %s", host)
    conn.open()

    if TABLE not in conn.tables():
        # Create a table with column family `cf` with default settings.
        conn.create_table(TABLE, {"cf": dict()})
        logging.debug("Created table %s", TABLE)
    else:
        logging.debug("Using table %s", TABLE)
    return happybase.Table(TABLE, conn)

def generate(args, table):
    b = table.batch()
    for time in get_time_range(args):
        b.put(time, {"cf:value": str(random.randint(0, 100000))})
    b.send()

def query(args, table):
    r = list(get_time_range(args))
    t = 0L
    for key, data in table.scan(row_start=min(r), row_stop=max(r)):
        if args.total:
            t += long(data["cf:value"])
        else:
            print "%s\t%s" % (key, data["cf:value"])
    if args.total:
        print "total\t%s" % t

def get_time_range(args):
    cal = calendar.Calendar()
    years = [args.year]
    months = [args.month] if args.month is not None else range(1, 1+12)

    for year in years:
        for month in months:
            if args.day is not None:
                days = [args.day]
            else:
                days = cal.itermonthdays(year, month)
            for day in days:
                if day > 0:
                    yield "%04d%02d%02d" % (year, month, day)

def main():
    parser = argparse.ArgumentParser(description="An HBase example", usage=USAGE)
    parser.add_argument("--action", metavar="ACTION", choices=("generate", "query"), required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--day", type=int, default=None)
    parser.add_argument("--total", action="store_true", default=False)

    args = parser.parse_args()
    table = connect()

    if args.day is not None and args.month is None:
        raise RuntimeError("Please, specify a month when specifying a day.")
    if args.day is not None and (args.day < 0 or args.day > 31):
        raise RuntimeError("Please, specify a valid day.")

    if args.action == "generate":
        generate(args, table)
    elif args.action == "query":
        query(args, table)
    else:
        raise RuntimeError("Unknown action `%s`" % args.action)

if __name__ == "__main__":
    main()

