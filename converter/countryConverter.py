import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

class CountryConverter:
	
	@classmethod
    def countryTagToText(cls, chart_type, country, duration):
        '''
        return country : html text (Global, Viet Nam ....)
        '''

        country_text = None

        url="https://spotifycharts.com/{}/{}/{}/latest".format(chart_type, country, duration)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        try:
            webpage_byte = urlopen(req).read()

            webpage = webpage_byte.decode('utf-8')
            soup = BeautifulSoup(webpage, 'html.parser')

            for child in soup.find("div",{"data-type":"country"}).children:
                try:
                    country_tag = child.find('li', {"class":"selected"})
                    country_text = country_tag.text
                except:
                    continue
        except:
            return country_text

        return country_text