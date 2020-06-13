import pandas as pd
import requests
import json
import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt
import gzip
import re
import os, gzip, sys

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

# Testing some stuff out (pls ignore, low code quality alert)
def getFuelGenData():
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
# # # # # # #

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

def add_station_data(station_df, weather_df, station_id):
    # state, lat, lon, elevation
    weather_df = weather_df.copy(deep = True)
    df_station_id = station_df.loc[station_df['station_id'] == station_id]
    weather_df['State'] = df_station_id.iloc[0]['STATE']
    weather_df['Lat'] = df_station_id.iloc[0]['LAT']
    weather_df['Lon'] = df_station_id.iloc[0]['LON']
    weather_df['Elev'] = df_station_id.iloc[0]['ELEV(M)']

    return weather_df

def get_data(station=None, start_year=2016, end_year=2018, **kwargs):
        '''
        FROM https://github.com/tagomatech/ETL/blob/master/gsod/gsodscrapper.py
        Get weather data from the internet as memory stream
        '''
        big_df = pd.DataFrame()

        for year in range(start_year, end_year+1):

            # Define URL
            url = 'http://www1.ncdc.noaa.gov/pub/data/gsod/' + str(year) + '/' + str(station) \
                + '-' + str(year) + '.op.gz'

            # Define data stream
            stream = requests.get(url)

            # Unzip on-the-fly
            decomp_bytes = gzip.decompress(stream.content)
            data= decomp_bytes.decode('utf-8').split('\n')

            # Remove start and end elements
            data.pop(0) # Remove first line header
            data.pop()  # Remove last element

            # Define lists
            (stn, wban, date, mean_tmp, mean_tmp_f, dew_point, dew_point_f,
             mean_sea_level_press, mean_sea_level_press_f, mean_stn_pressure, mean_stn_pressure_f, visib, visib_f,
             mean_wind_speed, mean_wind_speed_f, max_wind_speed, max_gust_speed, max_tmp, max_tmp_f, min_tmp, min_tmp_f,
             prcp, prcp_f, snow_depth, fog_f, rain_or_drizzle_f, snow_or_ice_pellets_f, hail_f, thunder_f, tornado_f) = ([] for i in range(30))

            # Fill in lists
            for i in range(0, len(data)):
                stn.append(data[i][0:6])
                wban.append(data[i][7:12])
                date.append(data[i][14:22])
                mean_tmp.append(data[i][25:30])
                mean_tmp_f.append(data[i][31:33])
                dew_point.append(data[i][36:41])
                dew_point_f.append(data[i][42:44])
                mean_sea_level_press.append(data[i][46:52])      # Mean sea level pressure
                mean_sea_level_press_f.append(data[i][53:55])
                mean_stn_pressure.append(data[i][57:63])      # Mean station pressure
                mean_stn_pressure_f.append(data[i][64:66])
                visib.append(data[i][68:73])
                visib_f.append(data[i][74:76])
                mean_wind_speed.append(data[i][78:83])
                mean_wind_speed_f.append(data[i][84:86])
                max_wind_speed.append(data[i][88:93])
                max_gust_speed.append(data[i][95:100])
                max_tmp.append(data[i][103:108])
                max_tmp_f.append(data[i][108])
                min_tmp.append(data[i][111:116])
                min_tmp_f.append(data[i][116])
                prcp.append(data[i][118:123])
                prcp_f.append(data[i][123])
                snow_depth.append(data[i][125:130])   # Snow depth in inches to tenth
                fog_f.append(data[i][132])          # Fog
                rain_or_drizzle_f.append(data[i][133])          # Rain or drizzle
                snow_or_ice_pellets_f.append(data[i][134])          # Snow or ice pallet
                hail_f.append(data[i][135])          # Hail
                thunder_f.append(data[i][136])         # Thunder
                tornado_f.append(data[i][137])         # Tornado or funnel cloud

            '''
            Replacements
            min_tmp_f & max_tmp_f
            blank   : explicit => e
            *       : derived => d
            '''
            max_tmp_f = [re.sub(pattern=' ', repl='e', string=x) for x in max_tmp_f] # List comprenhension
            max_tmp_f = [re.sub(pattern='\*', repl='d', string=x) for x in max_tmp_f]

            min_tmp_f = [re.sub(pattern=' ', repl='e', string=x) for x in min_tmp_f]
            min_tmp_f = [re.sub(pattern='\*', repl='d', string=x) for x in min_tmp_f]

            # Create intermediate matrix
            mat = np.matrix(data=[stn, wban, date, mean_tmp, mean_tmp_f, dew_point, dew_point_f,
                   mean_sea_level_press, mean_sea_level_press_f, mean_stn_pressure, mean_stn_pressure_f, visib, visib_f,
                   mean_wind_speed, mean_wind_speed_f, max_wind_speed, max_gust_speed, max_tmp, max_tmp_f, min_tmp, min_tmp_f,
                   prcp, prcp_f, snow_depth, fog_f, rain_or_drizzle_f, snow_or_ice_pellets_f, hail_f, thunder_f, tornado_f]).T

            # Define header names
            headers = ['stn', 'wban', 'timestamp', 'mean_tmp', 'mean_tmp_f', 'dew_point', 'dew_point_f', 'mean_sea_level_press',
                        'mean_sea_level_press_f', 'mean_stn_pressure', 'mean_stn_pressure_f', 'visib', 'visib_f', 'mean_wind_speed',
                        'mean_wind_speed_f', 'max_wind_speed', 'max_gust_speed', 'max_tmp', 'max_tmp_f', 'min_tmp', 'min_tmp_f',
                        'prcp', 'prcp_f', 'snow_depth', 'fog_f', 'rain_or_drizzle_f', 'snow_or_ice_pellets_f', 'hail_f', 'thunder_f', 'tornado_f']

            # Set precision
            pd.set_option('precision', 3)

            # Create dataframe from matrix object
            df = pd.DataFrame(data=mat, columns=headers)

            # Replace missing values with NAs
            df = df.where(df != ' ', 9999.9)

            # Create station ids
            df['STATION_ID'] = df['stn'].map(str) + '-' + df['wban'].map(str)
            df = df.drop(['stn', 'wban'], axis=1)

            # Convert to numeric
            df[['mean_tmp', 'mean_tmp_f', 'dew_point', 'dew_point_f', 'mean_sea_level_press', 'mean_sea_level_press_f',
                'mean_stn_pressure', 'mean_stn_pressure_f', 'visib', 'visib_f', 'mean_wind_speed', 'mean_wind_speed_f',
                'max_wind_speed',  'max_gust_speed', 'max_tmp', 'min_tmp', 'prcp', 'snow_depth']] = df[['mean_tmp', 'mean_tmp_f', 'dew_point',
                                                                       'dew_point_f', 'mean_sea_level_press', 'mean_sea_level_press_f', 'mean_stn_pressure',
                                                                       'mean_stn_pressure_f', 'visib', 'visib_f', 'mean_wind_speed',
                                                                       'mean_wind_speed_f', 'max_wind_speed', 'max_gust_speed', 'max_tmp',
                                                                       'min_tmp', 'prcp', 'snow_depth']].apply(pd.to_numeric)
            # Replace missing weather data with NaNs
            df = df.replace(to_replace=[99.99, 99.9,999.9,9999.9], value=np.nan)

            # Convert to date format
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d')

            big_df = pd.concat([big_df, df])

        #print(big_df.head())

        # Add weather station information to the dataframe
        # df_isd = self._ISDwXstationSlist() # OK print(df_isd.head())
        #
        # stn_id = ''.join(str(x) for x in big_df.STATION_ID.head(1).values)
        # big_df['ctry'] = np.repeat(df_isd.CTRY[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        #
        # big_df['state'] = np.repeat(df_isd.STATE[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        # big_df['station_name'] = np.repeat(df_isd.STATION_NAME[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        # big_df['lat'] = np.repeat(df_isd.LAT[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        # big_df['lon'] = np.repeat(df_isd.LON[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        # # Added ELEV
        # big_df['elevation'] = np.repeat(df_isd.ELEV[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        # big_df['begin'] = np.repeat(df_isd.BEGIN[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        # big_df['end'] = np.repeat(df_isd.END[df_isd.STATION_ID == stn_id].values, big_df.shape[0])
        #
        # big_df.columns = map(str.lower, big_df.columns)
        #
        # #return big_df
        #
        # # Make transformations on data, clean, convert
        # df = self._dataCleanerConverter(big_df)
        print('Data for ' + station)
        return big_df


    # def _dataCleanerConverter(self, df):
    #     """
    #     '''
    #     FROM https://github.com/tagomatech/ETL/blob/master/gsod/gsodscrapper.py
    #
    #     Precipitation: transform prcp based on prcp_f, then convert to inch. to mm
    #     A = 1 report of 6-hour precipitation amount.
    #     B = Summation of 2 reports of 6-hour precipitation amount.
    #     C = Summation of 3 reports of 6-hour precipitation amount.
    #     D = Summation of 4 reports of 6-hour precipitation amount.
    #     E = 1 report of 12-hour precipitation amount.
    #     F = Summation of 2 reports of 12-hour precipitation amount.
    #     G = 1 report of 24-hour precipitation amount.
    #     H = Station reported '0' as the amount for the day (eg, from 6-hour reports),
    #         but also reported at least one occurrence of precipitation in hourly observations
    #         --this could indicate a trace occurred, but should be considered as incomplete data for the day.
    #     I = Station did not report any precip data for the day and did not report any occurrences of
    #         precipitation in its hourly observations--it's still possible that precip occurred but was not reported.
    #     '''
    #     df['prcp_fct'] = np.nan
    #     df.loc[df.prcp_f == 'A', 'prcp_fct'] = 4
    #     df.loc[df.prcp_f == 'B', 'prcp_fct'] = 2
    #     df.loc[df.prcp_f == 'C', 'prcp_fct'] = 4/3
    #     df.loc[df.prcp_f == 'D', 'prcp_fct'] = 1
    #     df.loc[df.prcp_f == 'E', 'prcp_fct'] = 2
    #     df.loc[df.prcp_f == 'F', 'prcp_fct'] = 1
    #     df.loc[df.prcp_f == 'G', 'prcp_fct'] = 1
    #     df.loc[df.prcp_f == 'H', 'prcp_fct'] = 1
    #     df.loc[df.prcp_f == 'I', 'prcp_fct'] = 1
    #     df.prcp_fct = df.prcp_fct.replace(np.nan, 1, regex=True)
    #     df.prcp = df.prcp * df.prcp_fct
    #     df = df.drop(labels='prcp_fct', axis=1)
    #     # Conversion to Europe/metric units
    #     df.prcp = df.prcp / 0.0393701
    #     df.mean_tmp  = (df.mean_tmp - 32) * 5 / 9
    #     # Replace NAs
    #     df.prcp = df.prcp.fillna(value=0)
    #     df.state = df.state.fillna(value='not_applicable')
    #     """
    #     return df

if __name__ == '__main__':
    main()
