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
    parser.add_argument('--dbaction', choices=['insert','update','delete','query'] )

    args = parser.parse_args()

    opts["chart_type"] = args.chart
    opts["duration"] = args.duration
    opts["country"] = args.country
    opts["date"] = args.date
    opts["db_action"] = args.dbaction

    if opts["country"] == None:
        opts["country"] = Config.country
    
    Scraper(opts)

    '''

    if opts["date"] == None:
        opts["date"] = DateExtractor(opts)

    Scraper(opts)
    '''

    '''
    chart_type_opts = ['regional', 'viral']
    duration_opts = ['weekly', 'daily']
    scrape_all_option = False
    scrape_latest_option = True
    scrape_confirm_option = False
    check_duplicate = False
    
    # get arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'harvwdlcp', ['help', 'all', 'regional', 'viral', 'weekly', 'daily', 'latest', 'check', 'duplicate'])
    except getopt.GetoptError as err:
        print (err)
        sys.exit(1)

    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            sys.exit()

        elif o in ('-r', '--regional'):
            chart_type_opts = ['regional']

        elif o in ('-v', '--viral'):
            chart_type_opts = ['viral']

        elif o in ('-w', '--weekly'):
            duration_opts = ['weekly']

        elif o in ('-d', '--daily'):
            duration_opts = ['daily']

        elif o in ('-a', '--all'):
            scraping_latest_option = False
            scraping_all_option = True
        
        elif o in ('-c', '--check'):
            scrape_latest_option = False
            scrape_confirm_option = True
        
        elif o in ('-p', '--duplicate'):
            check_duplicate = True
            scrape_latest_option = False
        
        else:
            assert False, 'unhandled option'
    
    country_extractor = CountryExtractor()
    scraper = Scraper()
    
    country_dict = country_extractor.extractCountryDict(chart_type_opts, duration_opts)
    
    if scrape_all_option == True:
        scraper.scrapingAllData(chart_type_opts, duration_opts, country_dict)
    elif scrape_latest_option == True:
        scraper.scrapingLatestData(chart_type_opts, duration_opts, country_dict)
    elif scrape_confirm_option == True:
        scraper.scrapingCheck(chart_type_opts, duration_opts, country_dict)

    if check_duplicate == True:
        scraper.checkDuplicate()
    '''

if __name__ == "__main__":
    sys.exit(main())
