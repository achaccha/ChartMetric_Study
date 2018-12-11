import argparse
import os
import sys
import json

from extractor.countryExtractor import CountryExtractor
from extractor.dateExtractor import DateExtractor
from scraper import Scraper
from config import Config

def main(argv=None):

    opts = {}

    parser = argparse.ArgumentParser()

    parser.add_argument('--chart', nargs='*', default=['viral', 'regional'])
    parser.add_argument('--duration', nargs='*', default=['weekly', 'daily'])
    parser.add_argument('--country', nargs='+')
    parser.add_argument('--date', nargs='+')
    
    parser.add_argument('--insert', action='store_true')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--delete', action='store_true')

    parser.add_argument('--set', nargs='*')

    parser.add_argument('--duplicate', action='store_true')

    args = parser.parse_args()

    opts["chart_type"] = args.chart
    opts["duration"] = args.duration
    opts["country"] = args.country
    opts["date"] = args.date

    opts["insert"] = args.insert
    opts["update"] = args.update
    opts["delete"] = args.delete

    opts["set"] = args.set
    opts["set_key"] = []
    opts["set_value"] = []

    if opts["set"]:
        if len(opts["set"]) % 2 == 1:
            print("There are key-value pairs that are not matched. Please try again")
            sys.exit()
        else:
            for arg in opts['set']:
                if opts['set'].index(arg) % 2 == 0:
                    opts["set_key"].append(arg)
                else:
                    opts["set_value"].append(arg)
    
    if opts["country"] == None:
        opts["country"] = Config.country
   
    Scraper(opts)

    if args.duplicate:
        Scraper.checkDuplicate()

if __name__ == "__main__":
    sys.exit(main())
