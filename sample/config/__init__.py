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

    #table = os.environ.get("TABLE") 
    table = "spotify_chart"

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

    country_dict = {
        'ad':'Andorra',
        'ar':'Argentina',
        'at':'Austria',
        'au':'Australia', 
        'be':'Belgium', 
        'bg':'Bulgaria', 
        'bo':'Bolivia', 
        'br':'Brazil', 
        'ca':'Canada', 
        'ch':'Switzerland', 
        'cl':'Chile', 
        'co':'Colombia', 
        'cr':'Costa Rica', 
        'cy':'Cyprus', 
        'cz':'Czech Republic',
        'de':'Germany', 
        'dk':'Denmark', 
        'do':'Dominican Republic', 
        'ec':'Ecuador', 
        'ee':'Estonia', 
        'es':'Spain', 
        'fi':'Finland', 
        'fr':'France', 
        'gb':'United Kingdom', 
        'global':'Global', 
        'gr':'Greece', 
        'gt':'Guatemala', 
        'hk':'Hong Kong', 
        'hn':'Honduras', 
        'hu':'Hungary', 
        'id':'Indonesia', 
        'ie':'Ireland', 
        'il':'Israel',
        'is':'Iceland', 
        'it':'Italy', 
        'jp':'Japan', 
        'li':'Liechtenstein', 
        'lt':'Lithuania', 
        'lu':'Luxembourg', 
        'lv':'Latvia', 
        'mc':'Monaco', 
        'mt':'Malta', 
        'mx':'Mexico', 
        'my':'Malaysia', 
        'ni':'Nicaragua', 
        'nl':'Netherlands', 
        'no':'Norway', 
        'nz':'New Zealand', 
        'pa':'Panama', 
        'pe':'Peru', 
        'ph':'Philippines', 
        'pl':'Poland',
        'pt':'Portugal', 
        'py':'Paraguay', 
        'ro':'Romania', 
        'se':'Sweden', 
        'sg':'Singapore', 
        'sk':'Slovakia', 
        'sv':'El Salvador', 
        'th':'Thailand', 
        'tr':'Turkey', 
        'tw':'Taiwan', 
        'us':'United States', 
        'uy':'Uruguay', 
        'vn':'Viet Nam', 
        'za':'South Africa'
    }
