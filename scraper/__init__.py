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
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
from collections import OrderedDict

class Scraper:

	@classmethod
	def __new__(cls, self, chart_type_opts, duration_opts, country_dict, date_dict):
		for chart_type in chart_type_opts:
			for duration in duration_opts:
				country_list = country_dict[chart_type]
				date_list = date_dict[chart_type+"_"+duration]
				cls.scraping(chart_type, country_list, duration, date_list)
				

	@classmethod
	def scraping(cls, chart_type, country_list, duration, date_list):		
		country_list = ['mc']

		for country in country_list:
			date_list = cls.renewDateList(chart_type, country, duration, date_list)

			for date in date_list:
				url = "https://spotifycharts.com/{}/{}/{}/{}".format(chart_type, country, duration, date)
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

									spotify_list = [spotify_track_id, spotify_rank, spotify_timestp, spotify_country, spotify_chart_type, spotify_duration]
									print(spotify_list)
								
								except:
									continue

				except HTTPError as e:
					print('HTTPError!')
					if e.getcode() == 500:
						content = e.read()
					else:
						raise

	@classmethod
	def renewDateList(cls, chart_type, country, duration, date_list):
		url = "https://spotifycharts.com/{}/{}/{}/{}".format(chart_type, country, duration, 'latest')
		req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

		try:
			webpage_byte = urlopen(req).read()
			webpage = webpage_byte.decode('utf-8')
			soup = BeautifulSoup(webpage, 'html.parser')
			selects = soup.find_all('li', {'class':'selected'})
			selects_list = [select['data-value'] for select in selects]
			select_date = selects_list[len(selects_list)-1]
			date_start_index = date_list.index(select_date)
			date_list = date_list[date_start_index:]
		except:
			date_list = []
		
		
		return date_list

	@classmethod
	def showListInfo(cls, daily_list, weekly_list, country_list):
		print(json.dumps(daily_list, indent=4))
		print("Daily_list count : ", len(daily_list))
		print("==========")
		print(json.dumps(weekly_list, indent=4))
		print("Weekly_list count : ", len(weekly_list))
		print("==========")
		print(json.dumps(country_list, indent=4))
		print("Country_list count : ", len(country_list))
		print("==========")
