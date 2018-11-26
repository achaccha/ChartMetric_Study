import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

class CountryExtractor:

    @classmethod
    def extractCountryDict(cls, chart_type_opts, duration_opts):
        country_dict = {}
        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_key = chart_type+"_"+duration
                country_dict[country_key] = cls.extractCountryTagList(chart_type, duration)

        return country_dict

    @classmethod
    def extractCountryTagList(cls, chart_type, duration):
        '''
        return country_list : html tag (us, vn...)

        * url을 request하는게 비효율적
        '''

        country_list = []

        url="https://spotifycharts.com/{}/global/weekly/latest".format(chart_type)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        webpage_byte = urlopen(req).read()

        webpage = webpage_byte.decode('utf-8')
        soup = BeautifulSoup(webpage, 'html.parser')

        for child in soup.find("div",{"data-type":"country"}).children:
            try:
                country_tags = child.find_all('li')
                if country_tags:
                    for country_tag in country_tags:
                        country = country_tag['data-value']
                        country_list.append(country)
            except:
                continue

        country_list = list(set(country_list))
        country_list.sort()

        return country_list
        