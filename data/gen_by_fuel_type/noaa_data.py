import pandas as pd
from datetime import datetime, timedelta
import pytz

states = ['DE', 'IL', 'IN', 'KY', 'MD', 'MI','NJ', 'NC', 'OH', 'PA', 'TN', 'VA', 'WV', 'DC']
weather_data = ['air-temp', 'dew_point_temp', 'sea_level_pressure', 'visibility', 'wind_speed', 'sky_ceiling_height']

# Define empty DF and change type so everything matches

for state in states:
    print(state)
    all_weather_df = pd.DataFrame(columns = ['day', 'hour', 'station'])
    all_weather_df['hour']=all_weather_df['hour'].astype(int)
    all_weather_df['station']=all_weather_df['station'].astype(str)

    for weather_elem in weather_data:

        df_weather = pd.read_csv('/root/noaa/' + state  + '/' + weather_elem + '.csv')
        df_weather = df_weather.drop(['committer', 'commit_hash', 'data_points', "avg", "min", "max", "median"], axis=1)

        df_weather = df_weather.melt(id_vars=["station", "commit_date"],
                var_name="hour",H
                value_name=weather_elem)

        df_weather['commit_date'] = df_weather['commit_date'].str.split(' ').str[0]
        df_weather['hour'] = df_weather['hour'].str.replace(r'hour', '')
        df_weather['hour']=df_weather['hour'].astype(int)
        df_weather['station']=df_weather['station'].astype(str)
        df_weather['commit_date'] =  pd.to_datetime(df_weather['commit_date'])
        df_weather = df_weather.rename(columns={'commit_date': 'day'})

        all_weather_df = pd.merge(all_weather_df, df_weather, how = 'outer', on = ['station','day', 'hour'])

    all_weather_df.to_csv("/root/noaa/" + state + "_weather.csv")
