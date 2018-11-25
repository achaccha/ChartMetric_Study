import getopt
import os
import sys
import json

from extractor.countryExtractor import CountryExtractor
from scraper import Scraper

# Main function
def main(argv=None):

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

if __name__ == "__main__":
    sys.exit(main())
