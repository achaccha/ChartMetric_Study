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

    '''
    TODO:
    scraping when given a specific date
    '''
    
    # get arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'harvwd', ['help', 'all', 'regional', 'viral', 'weekly', 'daily'])
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

        else:
            assert False, 'unhandled option'
    
    country_dict = Extractor(chart_type_opts, duration_opts)
    result = Scraper(chart_type_opts, duration_opts, country_dict)
    
    ''' 
    result = [['6JjPBQfI2Y8nIjnm65X6Pw', 197, '11/08/2018', 'Global', 'regional', 'weekly'],\
            ['2ZRo7axmMPeSVUvDbGkJah', 200, '11/08/2018', 'Global', 'regional', 'weekly'],\
            ['6JjPBQfI2Y8nIjnm65X6Pw', 199, '11/08/2018', 'Global', 'regional', 'weekly']]
    

    DBManager(result)
    '''

if __name__ == "__main__":
    sys.exit(main())
