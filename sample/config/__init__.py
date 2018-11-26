from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

class Config:

    postgres = {
        'dbname' : os.environ.get("DB_NAME"),
        'user' : os.environ.get("USER"),
        'host' : os.environ.get("HOST"),
        'password' : os.environ.get("PASSWORD")
    }

    table = os.environ.get("TABLE")

    country = {
    	# 65
    	'regional_daily' : ['ad', 'ar', 'at', 'au', 'be', 'bg', 'bo', 'br', 'ca', 'ch', 'cl', 'co', 'cr', 'cy', 'cz', 
    		'de', 'dk', 'do', 'ec', 'ee', 'es', 'fi', 'fr', 'gb', 'global', 'gr', 'gt', 'hk', 'hn', 'hu', 'id', 'ie', 'il', 
    		'is', 'it', 'jp', 'lt', 'lu', 'lv', 'mc', 'mt', 'mx', 'my', 'ni', 'nl', 'no', 'nz', 'pa', 'pe', 'ph', 'pl', 'pt', 
    		'py', 'ro', 'se', 'sg', 'sk', 'sv', 'th', 'tr', 'tw', 'us', 'uy', 'vn', 'za'
    	],

		# 65
    	'regional_weekly' : ['ad', 'ar', 'at', 'au', 'be', 'bg', 'bo', 'br', 'ca', 'ch', 'cl', 'co', 'cr', 'cy', 'cz', 
    		'de', 'dk', 'do', 'ec', 'ee', 'es', 'fi', 'fr', 'gb', 'global', 'gr', 'gt', 'hk', 'hn', 'hu', 'id', 'ie', 'il', 
    		'is', 'it', 'jp', 'lt', 'lu', 'lv', 'mc', 'mt', 'mx', 'my', 'ni', 'nl', 'no', 'nz', 'pa', 'pe', 'ph', 'pl', 'pt', 
    		'py', 'ro', 'se', 'sg', 'sk', 'sv', 'th', 'tr', 'tw', 'us', 'uy', 'vn', 'za'
    	],

    	# 66
    	'viral_daily' : ['ad', 'ar', 'at', 'au', 'be', 'bg', 'bo', 'br', 'ca', 'ch', 'cl', 'co', 'cr', 'cy', 'cz', 
    		'de', 'dk', 'do', 'ec', 'ee', 'es', 'fi', 'fr', 'gb', 'global', 'gr', 'gt', 'hk', 'hn', 'hu', 'id', 'ie', 'il', 
    		'is', 'it', 'jp', 'li', 'lt', 'lu', 'lv', 'mc', 'mt', 'mx', 'my', 'ni', 'nl', 'no', 'nz', 'pa', 'pe', 'ph', 'pl', 
    		'pt', 'py', 'ro', 'se', 'sg', 'sk', 'sv', 'th', 'tr', 'tw', 'us', 'uy', 'vn', 'za'
    	],

    	# 66
    	'viral_weekly' : ['ad', 'ar', 'at', 'au', 'be', 'bg', 'bo', 'br', 'ca', 'ch', 'cl', 'co', 'cr', 'cy', 'cz', 
    		'de', 'dk', 'do', 'ec', 'ee', 'es', 'fi', 'fr', 'gb', 'global', 'gr', 'gt', 'hk', 'hn', 'hu', 'id', 'ie', 'il', 
    		'is', 'it', 'jp', 'li', 'lt', 'lu', 'lv', 'mc', 'mt', 'mx', 'my', 'ni', 'nl', 'no', 'nz', 'pa', 'pe', 'ph', 'pl', 
    		'pt', 'py', 'ro', 'se', 'sg', 'sk', 'sv', 'th', 'tr', 'tw', 'us', 'uy', 'vn', 'za'
    	]
    }
