"""
Collect video metadata via TikTok Research API
"""

import requests
import json
from datetime import datetime, timedelta
import sys
import time
import csv 
import pandas as pd
import os
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

def append_to_existing_or_create_new(df, combined_df_path):
    if os.path.exists(combined_df_path):
        combined_df = pd.read_csv(combined_df_path)
    else:
        combined_df = pd.DataFrame()

    for date in df['utc_date_string'].unique():
        df_date = df[df['utc_date_string'] == date]
        # Define the file path for the current date (files are organized by date)
        date_file_path = f"/path/to/file/ecuador_{date}.csv"

        if os.path.exists(date_file_path):
            existing_df = pd.read_csv(date_file_path)
            updated_df = pd.concat([existing_df, df_date], ignore_index=True)
            updated_df = updated_df.drop_duplicates(subset=['id'], keep='first')
            updated_df.to_csv(date_file_path, index=False)
        else:
            df_date.to_csv(date_file_path, index=False)

        combined_df = pd.concat([combined_df, df_date], ignore_index=True)

    combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
    print("length of final/combined/ecuador csv file is:",len(combined_df))
    combined_df.to_csv(combined_df_path, index=False)


def save_to_json_file(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


def createURL(username,videoid):
    return f"https://www.tiktok.com/@{username}/video/{videoid}"


#Convert from Linux Epoch time to UTC time
def convert_epoch_to_datetime(input_time):
    utc_time_stamp = datetime.utcfromtimestamp(input_time)
    year = utc_time_stamp.year
    month = utc_time_stamp.month
    day = utc_time_stamp.day
    hour = utc_time_stamp.hour
    minute = utc_time_stamp.minute
    second = utc_time_stamp.second
    date_string = utc_time_stamp.strftime("%Y-%m-%d")
    time_string = utc_time_stamp.strftime("%H:%M:%S")

    return pd.Series([year,month,day,hour,minute,second,date_string,time_string])


def fetch_tiktok_data(start_date,end_date,keywordsList,hashtagsList):
    count = 0
    full_json_response = {}
    full_json_response['data']={}
    full_json_response['data']['videos'] = [] 

    headers = {
        'authorization': 'Bearer <INSERT TOKEN HERE>'
    }

    url = 'https://open.tiktokapis.com/v2/research/video/query/?fields=id,like_count,create_time,region_code,share_count,view_count,like_count,comment_count,music_id,hashtag_names,username,effect_ids,playlist_id,video_description,voice_to_text'
    
    data = {
            "query": {
                "and": [
                    { "operation": "IN", "field_name": "region_code", "field_values": ["EC"] },
                ],
                "or":[
                    { "operation": "IN", "field_name": "keyword", "field_values": keywordsList },
                    { "operation": "IN", "field_name": "hashtag_name", "field_values": hashtagsList },
                ]
            }, 
            "start_date":start_date,
            "end_date":end_date,
            "max_count": 100 
    }
    max_retries = 3  # Set the maximum number of retries
    retries = 0
    total_count = 0

    while True:
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                if response.json()["data"]["has_more"] != True:
                    full_json_response['data']['videos'].extend(response.json()['data']['videos'])
                    return full_json_response,total_count
                elif response.json()["data"]["has_more"] == True:
                    next_cursor = response.json()['data']['cursor']
                    next_search_id = response.json()['data']['search_id']
                    data['cursor'] = next_cursor
                    data['search_id'] = next_search_id
                    full_json_response['data']['videos'].extend(response.json()['data']['videos'])
                    total_count += len(response.json()['data']['videos'])

            elif response.status_code == 401:
                print("Error:",response.status_code,response.text)
                return full_json_response,total_count
            elif response.status_code == 429: 
                print("Error:",response.status_code,response.text)
                return full_json_response,total_count
            elif response.status_code == 500: 
                print("Error:",response.status_code,response.text)
                time.sleep(100)
            elif response.status_code == 503: 
                print("Error:",response.status_code,response.text)
                time.sleep(100)
            elif response.status_code == 504: 
                print("Error:",response.status_code,response.text)
                time.sleep(200)
            else:
                print("Error:", response.status_code, response.text)
                return full_json_response,total_count
        
        except (ChunkedEncodingError, ProtocolError) as e:
            retries += 1
            if retries >= max_retries:
                print(f"Max retries reached. Last error: {e}")
                return full_json_response, total_count
            print(f"Encountered an error: {e}. Retrying ({retries}/{max_retries})...")
            time.sleep(2 ** retries)  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error: {e}")
            return full_json_response, total_count
    

start_date="YYYYMMDD" #insert start date
start_date_obj = datetime.strptime(start_date, "%Y%m%d")

with open('supplementary/keywords_hashtags.txt', 'r') as file:
    lines = [line.strip() for line in file if line.strip()]

keywordsList= lines
hashtagsList = lines

while start_date != "YYYYMMDD": #insert end date 
    end_date_obj = start_date_obj + timedelta(days=1)
    end_date_str = end_date_obj.strftime("%Y%m%d")
    data,total = fetch_tiktok_data(start_date,end_date_str,keywordsList=keywordsList,hashtagsList=hashtagsList)

    if total!= 0:
        # put the data into a json file 
        if data:
            save_to_json_file(data, f'{start_date}_{end_date_str}_ecuador.json')
        #put data into csv file
        if data:
            df = pd.DataFrame(data['data']['videos'])
            #Add the tiktok urls
            df['tiktokurl'] = df.apply(lambda row: createURL(row['username'], row['id']), axis=1)
            df[['utc_year','utc_month','utc_day','utc_hour','utc_minute','utc_second','utc_date_string','utc_time_string']] = df['create_time'].apply(convert_epoch_to_datetime)
            append_to_existing_or_create_new(df, "file/path/to/ecuador.csv")

    #move on to the next day
    start_date_obj = datetime.strptime(start_date, "%Y%m%d")
    start_date_obj = start_date_obj + timedelta(days=1)
    start_date = start_date_obj.strftime("%Y%m%d")