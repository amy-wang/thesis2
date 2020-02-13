from wwo_hist import retrieve_hist_data

import os
os.chdir("weather/")

frequency=1
start_date = '1-JAN-2008'
end_date = '31-DEC-2017'
api_key = 'f718589172844adfb3e195911202801'
location_list = ['Florida','California', 'Oregon', 'Washington', 'Nevada', 'Montana', 'Idaho', 'Wyoming', 'utah', 'Colorado', 'Arizona', 'New Mexico', 'Texas', 'Kansas', 'Oklahoma', 'Louisiana', 'Arkansas', 'Wisconsin', 'Michigan', 'Illinois', 'Indiana', 'Kentucky', 'Ohio', 'West Virginia', 'Virginia', 'Maryland', 'Pennsylvania', 'Pennsylvania', 'New Jersey', 'New York', 'Connecticut', 'Delaware', 'Rhode Island', 'Massachusetts', 'Vermont', 'New Hampshire', 'Maine', 'Mississippi', 'Tennessee', 'Alabama', 'Georgia', 'North Carolina', 'South Carolina', 'Iowa', 'Minnesota', 'North Dakota', 'Nebraska', 'South Dakota']

hist_weather_data = retrieve_hist_data(api_key,
                                location_list,
                                start_date,
                                end_date,
                                frequency,
                                location_label = False,
                                export_csv = True,
                                store_df = True)
