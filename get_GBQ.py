import pandas as pd
import pandas_gbq as pdq
from google.oauth2 import service_account
import datetime

FILEWAY = './last_online.csv'

print(f'Запуск скрипта в {str(datetime.datetime.now())}')

# получение данный с GBQ
credentials = service_account.Credentials.from_service_account_file('*.json')
query = """SELECT event_name,
            cast(timestamp_micros(event_timestamp) as datetime) as datetime_UTC, 
            date_add(cast(timestamp_micros(event_timestamp) as datetime),interval 3 HOUR) as datetime_MSC, 
            user_pseudo_id, 
            geo.country, 
            geo.city, 
            device.web_info.browser,
            device.mobile_marketing_name,
            device.mobile_brand_name,
            device.operating_system,
            traffic_source.name,
            traffic_source.medium,
            traffic_source.source
            FROM `*`
            where cast(timestamp_micros(event_timestamp) as datetime)>= date_add(CURRENT_DATEtime(), interval -7 DAY) AND event_name = 'session_start' or 
            cast(timestamp_micros(event_timestamp) as datetime)>= date_add(CURRENT_DATEtime(), interval -7 DAY) AND event_name = 'first_visit' or 
            cast(timestamp_micros(event_timestamp) as datetime)>= date_add(CURRENT_DATEtime(), interval -7 DAY) AND event_name = 'registration' or
            cast(timestamp_micros(event_timestamp) as datetime)>= date_add(CURRENT_DATEtime(), interval -7 DAY) AND event_name = 'dep_success_transaction_5'"""
project_id = 'glory-casino-ga4-analytics'
df = pdq.read_gbq(query, project_id=project_id, credentials=credentials)

print(f'Данные получены с GBQ в {str(datetime.datetime.now())}')

# Кладем данные в csv
df.to_csv(FILEWAY, index=False)

print(f'CSV файл обновлен в {str(datetime.datetime.now())}')