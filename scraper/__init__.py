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
    def scrapingAllData(cls, chart_type_opts, duration_opts, country_dict):
        db = DBManager()
        extractor = Extractor()

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                for country in country_list:

                    alert_msg = ">>>>>Scraping Start : {chart_type}_{country}_{duration}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration) 
                    cls.slackAlert(alert_msg)
                    
                    date_tag_list = extractor.extractDateTagList(chart_type, country, duration)
                    result = cls.scraping(chart_type, country, duration, date_tag_list)

                    db.insertData(result)
                    total = db.getTotalData()

                    alert_msg = ">>>>>Scraping End : {chart_type}_{country}_{duration}, Country result : {country_result}, Total result : {total}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, country_result=len(result), total=total) 
                    cls.slackAlert(alert_msg)

                db.closeConnection()
                cls.slackAlert(" ******** Finished!! ******** \n")
        
        return True


    @classmethod
    def scrapingLatestData(cls, chart_type_opts, duration_opts, country_dict):
        db = DBManager()
        extractor = Extractor()

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                for country in country_list:

                    alert_msg = ">>>>>Scraping Start : {chart_type}_{country}_{duration}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration) 
                    cls.slackAlert(alert_msg)
                    
                    date_tag_list = extractor.extractDateTagList(chart_type, country, duration)

                    country_text = extractor.countryTagToText(chart_type, country, duration)
                    date_db = db.getLatestDate(chart_type, country_text, duration)

                    if date_db == -1:
                        continue
                    
                    date_text = extractor.dateDBToText(date_db)
                    date_tag = extractor.dateTextToTag(chart_type, country, duration, date_text)

                    date_tag_index = date_tag_list.index(date_tag)
                    date_tag_list = date_tag_list[date_tag_index+1:]
                    
                    alert_msg = "***** New Date List in {chart_type}_{country}_{duration} : {latest_date_list}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, latest_date_list=date_tag_list)
                    cls.slackAlert(alert_msg)

                    country_result = cls.scraping(chart_type, country, duration, date_tag_list)

                    db.insertData(country_result)
                    total = db.getTotalData()

                    alert_msg = ">>>>>Scraping End : {chart_type}_{country}_{duration}, Country result : {country_result}, Total result : {total}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, country_result=len(country_result), total=total) 
                    cls.slackAlert(alert_msg)

                db.closeConnection()
                cls.slackAlert(" ******** Finished!! ******** \n")
        
        return True

    @classmethod
    def scrapingCheck(cls, chart_type_opts, duration_opts, country_dict):
        db = DBManager()
        extractor = Extractor()

        for chart_type in chart_type_opts:
            for duration in duration_opts:
                country_list = country_dict[chart_type+"_"+duration]
                for country in country_list:

                    alert_msg = ">>>>>Scraping Start : {chart_type}_{country}_{duration}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration) 
                    cls.slackAlert(alert_msg)
                    
                    country_text = extractor.countryTagToText(chart_type, country, duration)

                    date_text_list = extractor.extractDateTextList(chart_type, country, duration)
                    date_db_list = db.getDateList(chart_type, country_text, duration, date_text_list)
                    
                    update_date_text_list = extractor.dateDBToTextList(date_db_list)
                    update_date_db_list = list(set(date_text_list)-set(update_date_text_list))
                    
                    update_date_db_list.sort()

                    date_tag_list = extractor.dateTextToTagList(chart_type, country, duration, update_date_db_list)

                    alert_msg = "***** No Content Date List in {chart_type}_{country}_{duration} : {update_date_list}\n"\
                        .format(chart_type=chart_type, country=country, duration=duration, update_date_list=date_tag_list)
                    cls.slackAlert(alert_msg)
                    
                    country_result = cls.scraping(chart_type, country, duration, date_tag_list)

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

    @classmethod
    def checkDuplicate(cls):
        db = DBManager()
        duplicate = db.isDuplicate()

        if duplicate == True:
            cls.slackAlert("Some duplicate data in database!!")
        else:
            cls.slackAlert("No duplicate data in database!!")


        db.closeConnection()
