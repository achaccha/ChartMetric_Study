'''

id : 					integer(Auto Increment Value) 		ok
spotify_track_id : 		string								?  (<a href="https://open.spotify.com/track/7uzmGiiJyRfuViKKK3lVmR" target="_blank">)
rank : 					integer								ok (<td class="chart-table-position">198</td>)
timestp : 				date								ok
country : 				string								ok 
chart_type : 			string								ok (regional or viral)
duration : 				string								ok (weekly or daily)


'''

'''
CREATE TABLE spotify_chart (
  id              			serial primary key,
  spotify_track_id			text not null,
  rank 						integer not null,
  timestp					date not null,
  country 					text not null,
  chart_type 				text not null,
  duration 					text not null
);
'''

import json
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from collections import OrderedDict

from manager.db import DBManager
from extractor import Extractor


class Scraper:

    @classmethod
    def slack_alert(cls, msg):
        link = "https://hooks.slack.com/services/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"
        data = "{\"text\": \"%MSG%\"}".replace("%MSG%", msg)
        os.system("curl -X POST -H 'Content-type: application/json' --data '{data}' {link}".format(data=data, link=link))

    @classmethod
    def __new__(cls, self, chart_type_opts, duration_opts, country_dict):
        result = []
        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                cls.scraping(chart_type, country_list, duration, result)
        
        return result

    @classmethod
    def scraping(cls, chart_type, country_list, duration, result):		
        db = DBManager()
        #country_list = ['sk']

        for country in country_list:
            alert_msg = "<<<<<Scraping Start : {chart_type}_{country}_{duration}>>>>>\n"\
                .format(chart_type=chart_type, country=country, duration=duration) 
            cls.slack_alert(alert_msg)
            
            date_list = Extractor.DateList(chart_type, country, duration)
            country_result = []

            for date in date_list:
                url = "https://spotifycharts.com/{chart_type}/{country}/{duration}/{date}"\
                    .format(chart_type=chart_type, country=country, duration=duration, date=date)
                req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})                

                try:
                    webpage_byte = urlopen(req).read()
                    webpage = webpage_byte.decode('utf-8')

                    soup = BeautifulSoup(webpage, 'html.parser')

                    chart_error = soup.find_all('div', {'class':'chart-error'})
                                    
                    if chart_error:
                        continue
                    
                    first_info = soup.find_all('li', {'class':'selected'})
                    second_info  = soup.find_all('tr')

                    first_info_list = []
                    
                    if first_info:
                        first_info_list = [info.text for info in first_info]
                    
                    if first_info_list:
                        first_info_list = list(OrderedDict.fromkeys(first_info_list))
                            
                    if second_info:
                        for info in second_info:
                            try:
                                spotify_url = info.find('a').get('href')
                                spotify_url_info = spotify_url.split("/")
                                spotify_url_len = len(spotify_url_info)

                                spotify_track_id = spotify_url_info[spotify_url_len-1]
                                spotify_rank =  info.find('td', {'class':'chart-table-position'}).text
                                spotify_chart_type = chart_type
                                spotify_country = first_info_list[0]
                                spotify_duration = first_info_list[1]
                                spotify_timestp = first_info_list[2]
                                spotify_list = [spotify_track_id, int(spotify_rank), spotify_timestp, spotify_country, spotify_chart_type, spotify_duration]
                                 
                                result.append(spotify_list)
                                country_result.append(spotify_list)

                            except:
                                continue
                except HTTPError as e:
                    alert_msg = '###### {chart_type}/{country}/{duration}/{date} : HTTPError!\n'.format(chart_type=chart_type, country=country, duration=duration, date=date)
                    cls.slack_alert(alert_msg)
                    if e.getcode() == 500:
                        content = e.read()
                    else:
                        raise

            alert_msg = ">>>>>Scraping End : {chart_type}_{country}_{duration}, Country result : {country_result}, Total result : {total}\n"\
                .format(chart_type=chart_type, country=country, duration=duration, country_result=len(country_result), total=len(result)) 
            cls.slack_alert(alert_msg)

            db.insert(country_result)

        db.close_connection()
        cls.slack_alert(" *** Finished!! \n")
        
        return result
    
