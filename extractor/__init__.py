import json
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

class Extractor:

    @classmethod
    def __new__(cls, self, chart_type_opts, duration_opts):
        country_dic = {}

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_key = chart_type+"_"+duration
                country_dic[country_key] = cls.CountryList(chart_type)

        return country_dic

    @classmethod
    def CountryList(cls, chart_type):
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
        return country_list

    @classmethod
    def DateList(cls, chart_type, country, duration):
        date_list = []

        url="https://spotifycharts.com/{}/{}/{}/latest".format(chart_type, country, duration)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        try:
            webpage_byte = urlopen(req).read()

            webpage = webpage_byte.decode('utf-8')
            soup = BeautifulSoup(webpage, 'html.parser')
        
            for child in soup.find("div",{"data-type":"date"}).children:
                try:
                    date_tags = child.find_all('li')
                    if date_tags:
                        for date_tag in date_tags:
                            date = date_tag['data-value']
                            date_list.append(date)
                except:
                    continue
        except:
            return date_list            

        return date_list

