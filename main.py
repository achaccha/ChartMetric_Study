import getopt
import os
import sys
import json

from extractor import Extractor
from scraper import Scraper
from manager.db import DBManager

# Main function
def main(argv=None):

    chart_type_opts = ['regional', 'viral']
    duration_opts = ['weekly', 'daily']
    scrape_all_option = True
    scrape_latest_option = False
    scrape_confirm_option = False
    
    # get arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'harvwdlc', ['help', 'all', 'regional', 'viral', 'weekly', 'daily', 'latest', 'confirm'])
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

        elif o in ('-l', '--latest'):
            scrape_all_option = False
            scrape_latest_option = True

        elif o in ('-c', '--confirm'):
            scrape_all_option = False
            scrape_confirm_option = True
        else:
            assert False, 'unhandled option'
    
    scraper = Scraper()
    extractor = Extractor()

    # country_dict가 사라짐

    country_dict = extractor.extractCountryList(chart_type_opts, duration_opts)
    
    if scrape_all_option == True:
        scraper.scrapeAllData(chart_type_opts, duration_opts, country_dict)
    elif scrape_latest_option == True:
        scraper.scrapeLatestData(chart_type_opts, duration_opts, country_dict)
    elif scrape_confirm_option == True:
        scraper.scrapeConfirm(chart_type_opts, duration_opts, country_dict)

if __name__ == "__main__":
    sys.exit(main())
