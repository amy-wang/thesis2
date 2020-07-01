

import pandas as pd
import requests
import json
import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt
import gzip
import re
import os, gzip, sys

from dask.distributed import Client
import dask



#add the access token you got from NOAA
TOKEN = 'lwBmDJlPCkqeMyurcljRSVQHVFVhhHDt'
PJM_STATES = ['DE', 'IL', 'IN', 'KY', 'MD', 'MI', 'NJ', 'NC', 'OH', 'PA', 'TN', 'VA', 'WV', 'DC']
COLS = ['timestamp', 'mean_tmp', 'mean_tmp_f', 'dew_point', 'dew_point_f',
       'mean_sea_level_press', 'mean_sea_level_press_f', 'mean_stn_pressure',
       'mean_stn_pressure_f', 'visib', 'visib_f', 'mean_wind_speed',
       'mean_wind_speed_f', 'max_wind_speed', 'max_gust_speed', 'max_tmp',
       'max_tmp_f', 'min_tmp', 'min_tmp_f', 'prcp', 'prcp_f', 'snow_depth',
       'fog_f', 'rain_or_drizzle_f', 'snow_or_ice_pellets_f', 'hail_f',
       'thunder_f', 'tornado_f', 'STATION_ID', 'State', 'Lat', 'Lon', 'Elev']
FUEL_GEN_COLS = ['datetime_beginning_utc', 'fuel_type', 'mw', 'is_renewable']

def main():
    weather_station_df = getPJMWeatherStations()
    weather_station_df.loc[weather_station_df['STATE'] == 'DE']
    weather_station_df
    weather_df = pd.DataFrame(columns = COLS)
    for _, row in weather_station_df.iterrows():
        station_id = row['station_id']
        weather_by_station = get_data(station=station_id)
        station_df = add_station_data(weather_station_df, weather_by_station, station_id)
        pd.concat([weather_df, station_df])

    station_df.to_csv('PJM_weather_2016_to_2018.csv', header=False)

    get_data(station='720378-00122')

def cleanWeatherData():
    weather_df = pd.read_csv('/home/amy/thesis/data/all_weather.csv')

    # how to do
    weather_dates = df_stations[(df_stations['BEGIN'] <= '2012-1-1') & (df_stations['END'] >= '2017-12-31')]

def saveFuelGenData():
    df_fuel_gen_2016 = pd.read_csv('/home/amy/thesis/data/gen_by_fuel_type/2016.csv')
    df_fuel_gen_2017 = pd.read_csv('/home/amy/thesis/data/gen_by_fuel_type/2017.csv')
    df_fuel_gen_2018 = pd.read_csv('/home/amy/thesis/data/gen_by_fuel_type/2018.csv')

    df_fuel_gen = pd.concat([df_fuel_gen_2016, df_fuel_gen_2017, df_fuel_gen_2018])

    # get total fuel per hour
    df_fuel_gen['datetime_beginning_utc'] =  pd.to_datetime(df_fuel_gen['datetime_beginning_utc'])
    df_fuel_gen = df_fuel_gen[FUEL_GEN_COLS]
    df_fuel_gen
    df_fuel_gen['total_mw'] = df_fuel_gen['mw'].groupby(df_fuel_gen['datetime_beginning_utc']).transform('sum')
    df_non_renewable_gen = df_fuel_gen.loc[df_fuel_gen['is_renewable'] == False]
    df_renewable_gen = df_fuel_gen.loc[df_fuel_gen['is_renewable'] == True]

    df_renewable_gen = df_renewable_gen.drop(['is_renewable'], axis=1)
    df_non_renewable_gen = df_non_renewable_gen.drop(['is_renewable'], axis=1)

    df_renewable_gen.to_csv('/home/amy/thesis/data/gen_by_fuel_type/processed/renewables.csv')
    df_non_renewable_gen.to_csv('/home/amy/thesis/data/gen_by_fuel_type/processed/non_renewables.csv')

def getPJMWeatherStations():
    df_stations = pd.read_csv('/home/amy/thesis/temperature/gsod-stations.csv')
    df_stations = df_stations[df_stations['STATE'].isin(PJM_STATES)]
    df_stations

    # Keep stations with data between 2012 to 2018
    df_stations['BEGIN']= pd.to_datetime(df_stations['BEGIN'], format="%Y%m%d")
    df_stations['END']= pd.to_datetime(df_stations['END'], format="%Y%m%d")
    df_stations = df_stations[(df_stations['BEGIN'] <= '2012-1-1') & (df_stations['END'] >= '2017-12-31')]

    # Create station ID column
    df_stations.WBAN = df_stations.WBAN.astype(int)
    df_stations.WBAN = df_stations.WBAN.astype(str)
    df_stations.WBAN = df_stations.WBAN.str.zfill(5)
    df_stations['station_id'] = df_stations.USAF + '-' + df_stations.WBAN

    return df_stations

# def add_station_data(station_df, weather_df, station_id):
#     # state, lat, lon, elevation
#     weather_df = weather_df.copy(deep = True)
#     df_station_id = station_df.loc[station_df['station_id'] == station_id]
#     weather_df['State'] = df_station_id.iloc[0]['STATE']
#     weather_df['Lat'] = df_station_id.iloc[0]['LAT']
#     weather_df['Lon'] = df_station_id.iloc[0]['LON']
#     weather_df['Elev'] = df_station_id.iloc[0]['ELEV(M)']

#     return weather_df

if __name__ == '__main__':
    main()
