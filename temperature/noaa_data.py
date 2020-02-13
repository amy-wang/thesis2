
#needed to make web requests
import requests

#store the data we get as a dataframe
import pandas as pd

#convert the response as a strcuctured json
import json

#mathematical operations on lists
import numpy as np

#parse the datetimes we get from NOAA
from datetime import datetime

#add the access token you got from NOAA
Token = 'lwBmDJlPCkqeMyurcljRSVQHVFVhhHDt'

#Long Beach Airport station
station_id = 'GHCND:USW00023129'