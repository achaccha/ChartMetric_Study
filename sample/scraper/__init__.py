'''
CREATE TABLE spotify_chart (
  id              			serial primary key,
  spotify_track_id			text not null,
  rank 						integer not null,
  timestp					date not null,
  country 					text not null,
  chart_type 				text not null,
  duration 					text not null

  # 10186998
);
'''
INSERT_SPOTIFY_CHART_QUERY="INSERT INTO spotify_chart (spotify_track_id, rank, timestp, country, chart_type, duration, streams) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s);"

INSERT_SPOTIFY_CHART_QUERY_WITH_STREAMS="INSERT INTO spotify_chart (spotify_track_id, rank, timestp, country, chart_type, duration) \
                        VALUES (%s, %s, %s, %s, %s, %s);"

SELECT_DATE_QUERY="SELECT DISTINCT timestp \
            FROM spotify_chart \
            WHERE country=%s and chart_type=%s and duration=%s"

SELECT_ID_RANGE_QUERY="SELECT min(id), max(id) \
                FROM spotify_chart \
                WHERE chart_type=%s and duration=%s and country=%s and timestp=%s;"

SELECT_MAX_ID_QUERY="SELECT id FROM spotify_chart \
                ORDER BY id DESC LIMIT 1;"

SELECT_DUPLICATE_QUERY="SELECT spotify_track_id, rank, timestp, country, chart_type, duration, count(*) \
            FROM spotify_chart \
            GROUP BY spotify_track_id, rank, timestp, country, chart_type, duration \
            HAVING count(*) > 1;"

UPDATE_SPOTIFY_CHART_SET_CLAUSE="UPDATE spotify_chart SET "

UPDATE_SPOTIFY_CHART_NO_SET_CLAUSE="UPDATE spotify_chart \
                SET spotify_track_id=(%s), rank=(%s) \
                WHERE id=(%s);"

UPDATE_SPOTIFY_CHART_NO_SET_CLAUSE_WITH_STREAMS="UPDATE spotify_chart \
                                            SET spotify_track_id=(%s), rank=(%s), streams=(%s) \
                                            WHERE id=(%s);"


import csv
import datetime
import errno
import json
import locale
import os
import pycountry
import sys

from bs4 import BeautifulSoup
from collections import OrderedDict
from dateutil.parser import parse

from urllib.request import Request, urlopen
from urllib.error import HTTPError
from socket import error as SocketError

from converter.countryConverter import CountryConverter
from converter.dateConverter import DateConverter

from extractor.dateExtractor import DateExtractor

from manager.db import DBManager

locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 

class Scraper:

    @classmethod
    def __new__(cls, self, opts):
        cls.scrapingData(opts)

    @classmethod
    def slackAlert(cls, msg):
        link = "https://hooks.slack.com/services/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"
        data = "{\"text\": \"%MSG%\"}".replace("%MSG%", msg)
        os.system("curl -X POST -H 'Content-type: application/json' --data '{data}' {link}".format(data=data, link=link))
    
    @classmethod
    def scrapingData(cls, opts):
        db = DBManager()
        date_converter = DateConverter()

        chart_type_list = opts['chart_type']
        duration_list = opts['duration']
        country_list = opts['country']
        date_list = opts['date']

        update_list = opts['update']
        
        # DB Update
        if opts['update']:

            if opts['set']:
                conn = db.getPostgres()
                cursor = conn.cursor()

                where_str = ""
                set_str = ""

                where_list = []
                set_list = []

                for index in range(len(opts['set_key'])):
                    set_str += opts['set_key'][index] + "= %s"
                    set_list.append(opts['set_value'][index])

                    if index != len(opts['set_key'])-1:
                        set_str += ','

                if len(opts['chart_type']) != 2:
                    where_str += "chart_type = %s"
                    where_list.append(opts['chart_type'][0])
                
                if len(opts['duration']) != 2:
                    if where_str != "":
                        where_str += " and "
                
                    where_str += "duration = %s"
                    where_list.append(opts['duration'][0])
                
                if type(opts['country']) != dict: 
                    if where_str != "":
                        where_str += " and "
                    
                    for country in country_list:
                        index = country_list.index(country)
                        if country == "global":
                            country_name = "Global"
                        else:
                            country_name = pycountry.countries.get(alpha_2=country.upper()).name
                            country_name = country_name.split(',')[0]

                        where_str += "country = %s"
                        where_list.append(country_name)
                        
                        if index != len(country_list)-1:
                            where_str += " or "
                    
                if opts['date'] != None:
                    if where_str != "":
                        where_str += " and "

                    for date in date_list:
                        index = date_list.index(date)
                        date = str(parse(date).date())
                        where_str += "timestp = %s"
                        where_list.append(date)

                        if index != len(date_list)-1:
                            where_str += " or "

                update_data = tuple(set_list + where_list)
                update_sql = UPDATE_SPOTIFY_CHART_SET_CLAUSE + set_str

                # DB Update
                if where_str != "":
                    update_sql += " WHERE " + where_str

                cursor.execute(update_sql, update_data)
                conn.commit()

            else:
                for chart_type in chart_type_list:
                    for duration in duration_list:
                        # Get country list from country_dict based on chart_type and duration
                        if type(opts['country']) == dict:
                            country_list = opts['country'][chart_type+"_"+duration]

                        for country in country_list:
                            if country == 'global':
                                country_name = 'Global'
                            else:
                                country_name = pycountry.countries.get(alpha_2=country.upper()).name
                                country_name = country_name.split(",")[0]
                            
                            if opts['date'] == None:
                                conn = db.getPostgres()
                                cursor = conn.cursor()

                                query_sql = SELECT_DATE_QUERY
                                query_data = (country_name, chart_type, duration,)
                                cursor.execute(query_sql, query_data)
                                records = cursor.fetchall()
                                
                                date_list = [parse(str(record[0])).date() for record in records]

                            alert_msg = ">>>>> UPDATING START : {chart_type}/{country}/{duration}"
                                .format(chart_type=chart_type, country=country, duration=duration)
                            cls.slackAlert(alert_msg)

                            for date in date_list:
                                conn = db.getPostgres()
                                cursor = conn.cursor()

                                query_sql = SELECT_ID_RANGE_QUERY
                                query_data = (chart_type, duration, country_name, date,)
                                cursor.execute(query_sql, query_data)
                                records = cursor.fetchall()

                                min_id = records[0][0]
                                max_id = records[0][1]
                                
                                # In this case there are no data.
                                if min_id == None or max_id == None:
                                    alert_msg = ">>>>> There are no data in DB: {chart_type}/{country}/{duration}/{date}"
                                        .format(chart_type=chart_type, country=country, duration=duration, date=date)
                                    cls.slackAlert(alert_msg)
                                    
                                    cursor.close()
                                    continue
                                
                                date_tag = date_converter.dateTextToTag(chart_type, duration, date)
                                result = cls.scraping(chart_type, country, duration, date_tag)

                                # New data has benn created, so you need to confirm.
                                if (max_id-min_id)+1 != len(result):
                                    alert_msg = ">>>>> There are some extra data! Please check it! : {chart_type}/{country}/{duration}/{date}"
                                        .format(chart_type=chart_type, country=country, duration=duration, date=date_tag)
                                    cls.slackAlert(alert_msg)
                                    
                                    cursor.close()
                                    continue

                                current_id = min_id
                                for item in result:
                                    spotify_track_id = item[0]
                                    rank = item[1]
                                    
                                    if len(item) != 7:
                                        update_sql = UPDATE_SPOTIFY_CHART_NO_SET_CLAUSE
                                        update_data = (spotify_track_id, rank, current_id,)
                                    else:
                                        streams = item[6]
                                        
                                        update_sql = UPDATE_SPOTIFY_CHART_NO_SET_CLAUSE_WITH_STREAMS
                                        update_data = (spotify_track_id, rank, streams, current_id,)
                                        
                                    cursor.execute(update_sql, update_data)
                                    conn.commit()
                                    
                                    current_id += 1

                            alert_msg = ">>>>> UPDATING END : {chart_type}/{country}/{duration}"
                                .format(chart_type=chart_type, country=country, duration=duration)
                            cls.slackAlert(alert_msg)
                                
        # DB Insert
        elif opts['insert']:
            for chart_type in chart_type_list:
                for duration in duration_list:
                    # Get country list from country_dict based on chart_type and duration
                    if type(opts['country']) == dict:
                        country_list = opts['country'][chart_type+"_"+duration]
                    
                    for country in country_list:
                        if country == 'global':
                            country_name = 'Global'
                        else:
                            country_name = pycountry.countries.get(alpha_2=country.upper()).name
                            country_name = country_name.split(",")[0]

                        alert_msg = ">>>>>Scraping Start : {chart_type}_{country}_{duration}\n"\
                            .format(chart_type=chart_type, country=country, duration=duration)
                        cls.slackAlert(alert_msg)
                        
                        # Default : scraping latest data
                        if opts['date'] == None: 
                            conn = db.getPostgres()
                            cursor = conn.cursor()

                            query_sql = SELECT_DATE_QUERY
                            query_data = (country_name, chart_type, duration,)
                            cursor.execute(query_sql, query_data)
                            records = cursor.fetchall()
                            
                            date_list = [parse(str(record[0])).date() for record in records]

                            date_list.sort(reverse=True)
                            
                            if date_list:
                                date_db = date_list[0]
                            else:
                                continue
                            
                            cursor.close()

                            date_tag_list = DateExtractor(chart_type, country, duration)
                            date_tag = date_converter.dateTextToTag(chart_type, duration, date_db)

                            date_tag_index = date_tag_list.index(date_tag)
                            date_tag_list = date_tag_list[date_tag_index+1:]
                        
                        else:
                            date_tag_list = []
                            for date in opts['date']:
                                date = parse(date).date()
                                date_tag = date_converter.dateTextToTag(chart_type, duration, date)
                                date_tag_list.append(date_tag)
                        
                        alert_msg = "***** New Date List in {chart_type}_{country}_{duration} : {latest_date_list}\n"\
                            .format(chart_type=chart_type, country=country, duration=duration, latest_date_list=date_tag_list)
                        cls.slackAlert(alert_msg)

                        for date in date_tag_list:
                            result = cls.scraping(chart_type, country, duration, date)

                            for item in result:
                                conn = db.getPostgres()
                                cursor = conn.cursor()

                                spotify_track_id = item[0]
                                rank = item[1]
                                timestp = item[2]
                                country = item[3]
                                chart_type = item[4]
                                duration = item[5]

                                insert_data = (spotify_track_id, rank, timestp, country, chart_type, duration,)

                                if len(result) == 7:
                                    streams = item[6]

                                    insert_data = insert_data + (streams,)
                                    insert_sql = INSERT_SPOTIFY_CHART_QUERY

                                else:
                                    insert_sql = INSERT_SPOTIFY_CHART_QUERY_WITH_STREAMS

                                cursor.execute(insert_sql, insert_data)
                                conn.commit()

                            cursor.close()


                        conn = db.getPostgres()
                        cursor = conn.cursor()

                        query_sql = SELECT_MAX_ID_QUERY
                        cursor.execute(query_sql)
                        records = cursor.fetchone()
                        total = records[0]

                        cursor.close()

                        alert_msg = ">>>>>Scraping End : {chart_type}/{country}/{duration}, Country result : {result}, Total result : {total}\n"\
                            .format(chart_type=chart_type, country=country, duration=duration, result=len(result), total=total)
                        cls.slackAlert(alert_msg)

        
        # DB Delete
        # elif opts['delete']:
        #    continue

        db.closeConnection()


    @classmethod
    def scraping(cls, chart_type, country, duration, date):

        result = []

        #http_csv = open("./csv/"+chart_type+"_"+duration+"_http.csv", 'a')
        #empty_csv = open("./csv/"+chart_type+"_"+duration+"_empty.csv", 'a')

        #http_wr = csv.writer(http_csv)
        #empty_wr = csv.writer(empty_csv)    

        url = "https://spotifycharts.com/{chart_type}/{country}/{duration}/{date}"\
            .format(chart_type=chart_type, country=country, duration=duration, date=date)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})                

        try:
            webpage_byte = urlopen(req).read()
            webpage = webpage_byte.decode('utf-8')

            soup = BeautifulSoup(webpage, 'html.parser')

            # This {chart_type, country, duration, date} is empty
            chart_error = soup.find_all('div', {'class':'chart-error'})
                            
            if chart_error:
                #empty_wr.writerow([chart_type, country, duration, date])
                raise

            # Extract selected country, duration, date
            first_info = soup.find_all('li', {'class':'selected'})
            
            # Extract url to get spotify_track_id, rank 
            second_info  = soup.find_all('tr')

            # Extract streaming data
            stream_info =  soup.find_all('td', {'class':'chart-table-streams'})

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

                    except:
                        continue
            
            if stream_info and len(result) == len(stream_info):
                for idx in range(len(stream_info)):
                    spotify_stream = locale.atoi(stream_info[idx].text)
                    result[idx].append(spotify_stream)

        except HTTPError as e:
            #http_wr.writerow([chart_type, country, duration, date])

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

        #http_wr.writerow([">>>>>>"])
        #empty_wr.writerow([">>>>>>"])

        #http_csv.close()
        #empty_csv.close()
       
        return result

    @classmethod
    def checkDuplicate(cls):
        db = DBManager()

        cls.slackAlert("==================Checking Duplicate==================")

        conn = db.getPostgres()
        cursor = conn.cursor()
        
        query_sql = SELECT_DUPLICATE_QUERY
        cursor.execute(query_sql)
        records = cursor.fetchone()

        conn.commit()
        cursor.close()

        if records != None:
            cls.slackAlert("Some duplicate data in database!!")
        else:
            cls.slackAlert("No duplicate data in database!!")


        cls.slackAlert("======================================================")
        db.closeConnection()
