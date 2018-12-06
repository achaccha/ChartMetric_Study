import datetime
import json

from dateutil.parser import parse
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

class DateExtractor:

    @classmethod
    def __new__(cls, self, chart_type, country, duration):
        return cls.extractDateTagList(chart_type, country, duration)

    @classmethod
    def extractDateTagList(cls, chart_type, country, duration):
        ''' 
        return date_list : html tag ( 2018-01-15--2018-01-22....)
        '''

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
        
        date_list = list(set(date_list))
        date_list.sort()

        return date_list

    @classmethod
    def extractDateTextList(cls, chart_type, country, duration):
        '''
        return date_list : html text ( 01/15/2018...)
        '''

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
                            date = date_tag.text 
                            date_list.append(date)
                except:
                    continue
        except:
            return date_list

        date_list = list(set(date_list))
        date_list.sort()

        return date_list

    @classmethod
    def dateTextToTag(cls, chart_type, duration, date):
        '''
        return date_tag ( ex) 2018-01-15--2018-01-22)
        '''
        if type(date) == str:
            date = parse(date).date()

        if chart_type == 'regional' and duration == 'weekly':
            date_tag = str(date-datetime.timedelta(days=6))+"--"+str(date+datetime.timedelta(days=1))
        elif chart_type == 'viral' and duration == 'weekly':
            date_tag = str(date)+"--"+str(date)
        else:
            date_tag = str(date)

        return date_tag
