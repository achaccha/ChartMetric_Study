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
import csv
import errno
import json
import os
import sys

from bs4 import BeautifulSoup
from collections import OrderedDict

from urllib.request import Request, urlopen
from urllib.error import HTTPError
from socket import error as SocketError

from manager.db import DBManager
from extractor import Extractor

class Scraper:

    @classmethod
    def slackAlert(cls, msg):
        link = "https://hooks.slack.com/services/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"
        data = "{\"text\": \"%MSG%\"}".replace("%MSG%", msg)
        os.system("curl -X POST -H 'Content-type: application/json' --data '{data}' {link}".format(data=data, link=link))

    @classmethod
    def scrapeAllData(cls, chart_type_opts, duration_opts, country_dict):
        db = DBManager()
        extractor = Extractor()

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                for country in country_list:

                    alert_msg = "<<<<<Scraping Start : {chart_type}_{country}_{duration}>>>>>\n"\
                        .format(chart_type=chart_type, country=country, duration=duration) 
                    cls.slackAlert(alert_msg)
                    
                    date_list = extractor.extractDateList(chart_type, country, duration)
                    country_result = cls.scraping(chart_type, country, duration, date_list)

                    db.insertData(country_result)
                    total = db.getTotalData()

                    alert_msg = ">>>>>Scraping End : {chart_type}_{country}_{duration}, Country result : {country_result}, Total result : {total}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, country_result=len(country_result), total=total) 
                    cls.slackAlert(alert_msg)

                db.closeConnection()
                cls.slackAlert(" ******** Finished!! ******** \n")
        
        return True


    @classmethod
    def scrapeLatestData(cls, chart_type_opts, duration_opts, country_dict):
        db = DBManager()
        extractor = Extractor()

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                for country in country_list:

                    alert_msg = "<<<<<Scraping Start : {chart_type}_{country}_{duration}>>>>>\n"\
                        .format(chart_type=chart_type, country=country, duration=duration) 
                    cls.slackAlert(alert_msg)
                    
                    country_name = extractor.extractCountryName(chart_type, country, duration)
                    previous_date = str(db.getLatestDate(chart_type, country_name, duration))
                    date_list = extractor.updateDateList(chart_type, country, duration, previous_date)

                    country_result = cls.scraping(chart_type, country, duration, date_list)

                    db.insertData(country_result)
                    total = db.getTotalData()

                    alert_msg = ">>>>>Scraping End : {chart_type}_{country}_{duration}, Country result : {country_result}, Total result : {total}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, country_result=len(country_result), total=total) 
                    cls.slackAlert(alert_msg)

                db.closeConnection()
                cls.slackAlert(" ******** Finished!! ******** \n")
        
        return True

    @classmethod
    def scrapeConfirm(cls, chart_type_opts, duration_opts, country_dict):
        db = DBManager()
        extractor = Extractor()

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                for country in country_list:

                    alert_msg = "<<<<<Scraping Start : {chart_type}_{country}_{duration}>>>>>\n"\
                        .format(chart_type=chart_type, country=country, duration=duration) 
                    cls.slackAlert(alert_msg)
                    
                    date_list = extractor.extractDateList(chart_type, country, duration)
                    update_date_list = db.getDateList(chart_type, country, duration, date_list)

                    country_result = cls.scraping(chart_type, country, duration, update_date_list)

                    db.insertData(country_result)
                    total = db.getTotalData()

                    alert_msg = ">>>>>Scraping End : {chart_type}_{country}_{duration}, Country result : {country_result}, Total result : {total}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, country_result=len(country_result), total=total) 
                    cls.slackAlert(alert_msg)

                db.closeConnection()
                cls.slackAlert(" ******** Finished!! ******** \n")
        
        return True


    @classmethod
    def scraping(cls, chart_type, country, duration, date_list):

        country_result = []

        http_csv = open(chart_type+"_"+duration+"_http.csv", 'a')
        empty_csv = open(chart_type+"_"+duration+"_empty.csv", 'a')

        http_wr = csv.writer(http_csv)
        empty_wr = csv.writer(empty_csv)    

        for date in date_list:
            url = "https://spotifycharts.com/{chart_type}/{country}/{duration}/{date}"\
                .format(chart_type=chart_type, country=country, duration=duration, date=date)
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})                

            try:
                webpage_byte = urlopen(req).read()
                webpage = webpage_byte.decode('utf-8')

                soup = BeautifulSoup(webpage, 'html.parser')

                # empty data
                chart_error = soup.find_all('div', {'class':'chart-error'})
                                
                if chart_error:
                    empty_wr.writerow([chart_type, country, duration, date])
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
                            
                            country_result.append(spotify_list)

                        except:
                            continue

            except HTTPError as e:
                #alert_msg = '###### {chart_type}/{country}/{duration}/{date} : HTTPError!\n'.format(chart_type=chart_type, country=country, duration=duration, date=date)
                #cls.slackAlert(alert_msg)

                http_wr.writerow([chart_type, country, duration, date])

                if e.getcode() == 500:
                    content = e.read()
                else:
                    raise

            except SocketError as e:
                alert_msg = '###### SocketError!\n'
                cls.slackAlert(alert_msg)

                if e.errno != errno.ECONNRESET:
                    raise 
                
                print(e.getcode())

        http_wr.writerow([">>>>>>"])
        empty_wr.writerow([">>>>>>"])

        http_csv.close()
        empty_csv.close()

        return country_result

    
                    
