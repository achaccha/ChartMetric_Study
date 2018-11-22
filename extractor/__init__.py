import json
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

class Extractor:

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

    @classmethod
    def extractCountryText(cls, chart_type, country, duration):
        '''
        return country : html text (Global, Viet Nam ....)
        '''

        country = None

        url="https://spotifycharts.com/{}/{}/{}/latest".format(chart_type, country, duration)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        try:
            webpage_byte = urlopen(req).read()

            webpage = webpage_byte.decode('utf-8')
            soup = BeautifulSoup(webpage, 'html.parser')

            for child in soup.find("div",{"data-type":"country"}).children:
                try:
                    date_tag = child.find('li', {"class":"selected"})
                    country = date_tag.text
                except:
                    continue
        except:
            return country

        return country

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
    def dateTextToTagList(cls, chart_type, country, duration, date_text_list):
        '''
        return date_list : html text ( 01/15/2018...)
        '''

        date_tag_list = []

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
                        for date_text in date_text_list:
                            for date_tag in date_tags:
                                if date_tag.text == date_text:
                                    date_tag = date_tag['data-value']
                                    date_tag_list.append(date_tag)
                                    break
                except:
                    continue
        except:
            return date_tag_list

        date_tag_list = list(set(date_tag_list))
        date_tag_list.sort()

        return date_tag_list

    @classmethod
    def dateTextToTag(cls, chart_type, country, duration, date_text):
        '''
        return date_tag ( ex) 2018-01-15--2018-01-22)
        '''

        date_tag = None

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
                        for tag in date_tags:
                            if tag.text == date_text:
                                date_tag = tag['data-value']
                                return date_tag
                except:
                    continue
        except:
            return date_tag

        return date_tag

    @classmethod
    def dateTextToDBList(cls, date_list):
        ''' 
        Date html text -> DB column 
        '''

        db_date_list = []

        for date in date_list:
            html_month = date.split('/')[0]
            html_date = date.split('/')[1]
            html_year = date.split('/')[2]
    
            db_date = html_year+'-'+html_month+'-'+html_date
            db_date_list.append(db_date)

        return db_date_list
    
    @classmethod
    def dateDBToTextList(cls, date_list):
        '''
        DB column -> Date html text
        '''

        html_date_list = []

        for date in date_list:
            db_year = date.split('-')[0]
            db_month = date.split('-')[1]
            db_date = date.split('-')[2]

            html_date = db_month+"/"+db_date+"/"+db_year
            html_date_list.append(html_date)
        
        return html_date_list

    @classmethod
    def dateDBToText(cls, date):
        db_year = date.split('-')[0]
        db_month = date.split('-')[1]
        db_date = date.split('-')[2]

        html_date = db_month+"/"+db_date+"/"+db_year

        return html_date

