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

from time import localtime, strftime

from bs4 import BeautifulSoup
from collections import OrderedDict
from dateutil.parser import parse

from urllib.request import Request, urlopen
from urllib.error import HTTPError
from socket import error as SocketError

from config import Config
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

        chart_type_list = opts['chart_type']
        duration_list = opts['duration']
        country_list = opts['country']
        date_list = opts['date']

        update_list = opts['update']
        
        # DB Update
        if opts['update']:

            # If the user gives the conditions to be used set clause
            if opts['set']:
                conn = db.getPostgres()
                cursor = conn.cursor()

                where_str = ""
                set_str = ""

                where_list = []
                set_list = []

                # Create set query string
                for index in range(len(opts['set_key'])):
                    set_str += opts['set_key'][index] + "= %s"
                    set_list.append(opts['set_value'][index])

                    if index != len(opts['set_key'])-1:
                        set_str += ','

                
                # If the user gives the conditions to be used where clause, create where query string
                # each of conditions (chart_type, duration, country, date)
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
                        country_name = Config.country_dict[country]

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

                # create update query sql and data
                update_data = tuple(set_list + where_list)
                update_sql = UPDATE_SPOTIFY_CHART_SET_CLAUSE + set_str

                # If there is a where string, append it
                if where_str != "":
                    update_sql += " WHERE " + where_str

                cursor.execute(update_sql, update_data)
                conn.commit()

            # If the user don't gives the conditions to be used set clause,
            # Scraper scrape based on where clause. (default : all data scraping)
            else:
                for chart_type in chart_type_list:
                    for duration in duration_list:
                        # Get country list from country_dict based on chart_type and duration
                        if type(opts['country']) == dict:
                            country_list = opts['country'][chart_type+"_"+duration]

                        for country in country_list:
                            country_name = Config.country_dict[country]
                            
                            # Get date list from DB based on country_name, chart_type, duration.
                            if opts['date'] == None:
                                conn = db.getPostgres()
                                cursor = conn.cursor()

                                query_sql = SELECT_DATE_QUERY
                                query_data = (country_name, chart_type, duration,)
                                cursor.execute(query_sql, query_data)
                                records = cursor.fetchall()
                                
                                date_list = [record[0] for record in records]

                            date_list = [parse(str(date)).date() for date in date_list]
                            date_list.sort(reverse=True)

                            alert_msg = ">>>>> UPDATING START : {chart_type}/{country}/{duration}" \
                                .format(chart_type=chart_type, country=country, duration=duration)
                            cls.slackAlert(alert_msg)


                            for date in date_list:
                                conn = db.getPostgres()
                                cursor = conn.cursor()

                                # Since update must be based on existing one, 
                                # finds the range of id existing in DB in current condition.
                                query_sql = SELECT_ID_RANGE_QUERY
                                query_data = (chart_type, duration, country_name, date,)
                                cursor.execute(query_sql, query_data)
                                records = cursor.fetchall()

                                min_id = records[0][0]
                                max_id = records[0][1]
                                
                                date_tag = DateExtractor.dateTextToTag(chart_type, duration, date)
                                result, _, _ = cls.scraping(chart_type, country, duration, date_tag)

                                # In this case, the number of data currently scraped and the number of data in DB are different.
                                if (max_id-min_id)+1 != len(result):
                                    alert_msg = ">>>>> Please check it! : {chart_type}/{country}/{duration}/{date}" \
                                        .format(chart_type=chart_type, country=country, duration=duration, date=date_tag)
                                    cls.slackAlert(alert_msg)
                                    
                                    cursor.close()
                                    continue

                                current_id = min_id
                                for item in result:
                                    spotify_track_id = item[0]
                                    rank = item[1]
                                    
                                    # Checking for streams data.
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

                            alert_msg = ">>>>> UPDATING END : {chart_type}/{country}/{duration}" \
                                .format(chart_type=chart_type, country=country, duration=duration)
                            cls.slackAlert(alert_msg)
        # DB Update end...
                    
        # DB Insert
        elif opts['insert']:
            http_error_list = []
            empty_error_list = []
            for chart_type in chart_type_list:
                for duration in duration_list:
                    # Get country list from country_dict based on chart_type and duration
                    if type(opts['country']) == dict:
                        country_list = opts['country'][chart_type+"_"+duration]
                    
                    for country in country_list:
                        cls.slackAlert("===========================================================================================") 

                        alert_msg = ">>>>>Scraping Start : {chart_type}/{country}/{duration}\n"\
                            .format(chart_type=chart_type, country=country, duration=duration)
                        cls.slackAlert(alert_msg)
                        
                        country_name = Config.country_dict[country]
                        
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
                                cls.slackAlert("***** No Data in this condition... *****")
                                continue
                            
                            cursor.close()

                            date_tag_list = DateExtractor(chart_type, country, duration)
                            if date_tag_list == []:
                                cls.slackAlert("***** There are some connection Error to get date_tag_list!!! Please check it.")
                                continue
                            
                            date_tag = DateExtractor.dateTextToTag(chart_type, duration, date_db)

                            date_tag_index = date_tag_list.index(date_tag)
                            date_tag_list = date_tag_list[date_tag_index+1:]
                        
                        else:
                            date_tag_list = []
                            for date in opts['date']:
                                date = parse(date).date()
                                date_tag = DateExtractor.dateTextToTag(chart_type, duration, date)
                                date_tag_list.append(date_tag)
                        
                        alert_msg = "***** Today Scraping Date List in {chart_type}/{country}/{duration} : {latest_date_list}\n"\
                            .format(chart_type=chart_type, country=country, duration=duration, latest_date_list=date_tag_list)
                        cls.slackAlert(alert_msg)

                        country_result = 0
                        for date in date_tag_list:
                            result, http_error, empty_error = cls.scraping(chart_type, country, duration, date)
                            http_error_list.append(http_error)
                            empty_error_list.append(empty_error)
                            
                            if len(result) == 0:
                                alert_msg = "***** No data in : {chart_type}/{country}/{duration}/{date}\n"\
                                    .format(chart_type=chart_type, country=country, duration=duration, date=date)
                                cls.slackAlert(alert_msg)
                                continue
                            
                            country_result += len(result)
                            
                            for item in result:
                                conn = db.getPostgres()
                                cursor = conn.cursor()

                                spotify_track_id = item[0]
                                spotify_rank = item[1]
                                spotify_timestp = item[2]
                                spotify_country = item[3]
                                spotify_chart_type = item[4]
                                spotify_duration = item[5]

                                insert_data = (spotify_track_id, spotify_rank, spotify_timestp, spotify_country, spotify_chart_type, spotify_duration,)

                                if len(item) == 7:
                                    spotify_streams = item[6]

                                    insert_data = insert_data + (spotify_streams,)
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

                        alert_msg = ">>>>>Scraping End : {chart_type}/{country}/{duration}, Country result : {country_result}, Total result : {total}\n"\
                            .format(chart_type=chart_type, country=country, duration=duration, country_result=country_result, total=total)
                        cls.slackAlert(alert_msg)
                        cls.slackAlert("===========================================================================================") 

            http_error_list = sum(http_error_list, [])
            empty_error_list = sum(empty_error_list, [])
            cls.errorReport(http_error_list, empty_error_list)

        db.closeConnection()


    @classmethod
    def scraping(cls, chart_type, country, duration, date):
        result = []
        http_error_list = []
        empty_error_list = []

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
                empty_error_list.append([chart_type, country, duration, date])
                return result

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
            http_error_list.append([chart_type, country, duration, date])
            
            if e.getcode() == 500:
                content = e.read()
            else:
                raise

        except SocketError as e:
            cls.slackAlert("###### SocketError!\n")

            if e.errno != errno.ECONNRESET:
                raise 
       
        return result, http_error_list, empty_error_list

    @classmethod
    def errorReport(cls,http_error_list, empty_error_list):
        date = strftime("%m%d_%H%M", localtime())

        http_directory = "./csv/http/%s" % date
        empty_directory = "./csv/empty/%s" % date

        if not os.path.isdir(http_directory):
            os.makedirs(http_directory)

        if not os.path.isdir(empty_directory):
            os.makedirs(empty_directory)

        http_csv = open(http_directory+".csv", 'a')
        empty_csv = open(empty_directory+".csv", 'a')

        http_wr = csv.writer(http_csv)
        empty_wr = csv.writer(empty_csv)

        for data in http_error_list:
            http_wr.writerow([data[0], data[1], data[2], data[3]])

        for data in empty_error_list:
            empty_wr.writerow([data[0], data[1], data[2], data[3]])

        http_wr.writerow([">>>>>>"])
        empty_wr.writerow([">>>>>>"])

        http_csv.close()
        empty_csv.close()


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
