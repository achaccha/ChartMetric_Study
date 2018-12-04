import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup

class DateConverter:

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
    def dateDBToText(cls, date):
        db_year = date.split('-')[0]
        db_month = date.split('-')[1]
        db_date = date.split('-')[2]

        html_date = db_month+"/"+db_date+"/"+db_year

        return html_date
    
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

    
