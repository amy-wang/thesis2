import os
import pandas as pd
import numpy as np

# start with isorto data
def main():
    data = pd.read_csv("../data/raw_data/emit_agg_by_isorto.csv")

    # Drop rows without a region
    data = data[pd.notnull(data['isorto'])]
    data['ts'] = pd.to_datetime(data['ts'])

    # Organize data
    data = data.set_index(['ts', 'isorto']).sort_index()
    data.index.names = ['DATE_UTC', 'isorto']

    # Conversions
    lbsConversion = 0.45359237
    tonsConversion = 907.185
    convertFromKg(data, lbsConversion, 'lbs')
    convertFromKg(data, tonsConversion, 'tons')

    # Difference data

def convertFromKg(inputData, conversionFactor, lbsOrTons):
    lbsCol = inputData.columns[inputData.columns.str.endswith(lbsOrTons)]
    for col in lbsCol:
        inputData[col] = inputData[col] * conversionFactor
    inputData.columns = [x.replace(lbsOrTons, 'kg') for x in inputData.columns]

if __name__ == '__main__':
    main()