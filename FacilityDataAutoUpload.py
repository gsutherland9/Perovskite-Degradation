
#####FOR ALL FIVE FACILITIES, BRING IN DATA FROM NREL WEBSITE API'S, CLEAN/CONVERT DATA, UPLOAD IT TO SQL SERVER EVERYDAY USING THIS SCRIPT IN TASK SCHEDULER

import pyodbc
import requests
import pandas as pd
from io import StringIO
import numpy as np
from datetime import datetime, timedelta


## ESTABLISH THE CONNECTION
# server = #####
# database = #####
# username = #####
# driver = #####

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes"

try:
    connection = pyodbc.connect(connection_string)
    print("Connection successful!")
except pyodbc.Error as e:
    print("Error in connection:", e)



#YESTERDAY'S DATE
def get_yesterday_date_yyyymmdd():
    yesterday = datetime.today() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")

yesterday_date = get_yesterday_date_yyyymmdd()



#DATE AND TIME CONVERSION
#ADD ZEROS FOR LESS THAN 3 DIGITS
def add_leading_zeros(value):
    if len(str(value)) == 1:
        return str(value).zfill(3)
    elif len(str(value)) == 2:
        return str(value).zfill(3)
    return str(value)

#DATE CONVERT
def convert_YYYYDDD_to_YYYYMMDD(yyyyddd):
    year = yyyyddd // 1000 
    day_of_year = yyyyddd % 1000 
    date = pd.Timestamp(year=year, month=1, day=1) + pd.DateOffset(days=day_of_year - 1)
    return date.strftime('%Y%m%d')

#TIME CONVERSION
def combine_date_and_time(row):
    date_str = row['YYYYMMDD']
    time_value = row['Timezone']
    
    #DETERMINE IF THE TIME VALUE IS IN MINUTES OR IN HHMM FORMAT 
    if time_value < 100: 
        hours = 0
        minutes = time_value
    else: 
        hours = time_value // 100
        minutes = time_value % 100
    
    #FORMAT TIME STRING
    time_str = f"{hours:02d}:{minutes:02d}"
    
    #COMBINE
    return pd.to_datetime(f"{date_str} {time_str}")


#DEW POINT CALCULATION
def calculate_dew_point(row):
    T = row['Air Temperature [deg C]']
    RH = row['Rel Humidity [%]']

    x = ((17.27*T) / (237.7+T)) + np.log(RH/100)
    return (237.7*x) / (17.27-x)


#KELVIN TO CELSIUS
def KtoC(temp):
    return temp-273.15


#RENAME DATAFRAME
df_rename = ['Source','Datetime', 'Global Horiz [W/m^2]', 'Direct Normal [W/m^2]', 'Diffuse Horiz [W/m^2]', 'Zenith Angle [degrees]', 'Azimuth Angle [degrees]', 'Air Temperature [deg C]', 'Dew Point Temp [deg C]',
    'Rel Humidity [%]', 'Station Pressure [mBar]', 'Avg Wind Speed [m/s]', 'Peak Wind Speed [m/s]', 'Avg Wind Direction [deg from N]', 'Logger Temp [deg C]', 'Logger Battery [VDC]']


###UNIVERSITY OF ARIZONA OASIS

api_url = f'https://midcdmz.nrel.gov/apps/data_api.pl?site=UAT&begin={yesterday_date}&end={yesterday_date}' 

response = requests.get(api_url)

if response.status_code == 200:
    
    csv_data = StringIO(response.text) 
    df = pd.read_csv(csv_data)

#DATETIME CALLS
df.rename(columns={'MST': 'Timezone'}, inplace=True)
df['DOY'] = df['DOY'].apply(add_leading_zeros)
df['YYYYDDD'] = (df['Year'].astype(str) + df['DOY'].astype(str)).astype(int)
df['YYYYMMDD'] = df['YYYYDDD'].apply(convert_YYYYDDD_to_YYYYMMDD)
df['Datetime'] = df.apply(combine_date_and_time, axis=1)

#DEW POINT CALL
df['Dew Point Temp [deg C]'] = df.apply(calculate_dew_point, axis=1)

#SITE NAME
df['Source'] = 'UAOASIS'

#COLUMN RENAME
df_finalUAOASIS = df[['Source','Datetime', 'Global Horiz [W/m^2]', 'Direct Normal [W/m^2]', 'Diffuse Horiz [W/m^2]', 'Zenith Angle [degrees]', 'Azimuth Angle [degrees]', 'Air Temperature [deg C]', 'Dew Point Temp [deg C]',
    'Rel Humidity [%]', 'Station Pressure [mBar]', 'Avg Wind Speed @ 3m [m/s]', 'Peak Wind Speed @ 3m [m/s]', 'Avg Wind Direction @ 3m [deg from N]', 'CR1000 Temp [deg C]', 'CR1000 Battery [VDC]']]
df_finalUAOASIS.columns = df_rename


###UNIVERSITY OF OREGON

api_url = f'https://midcdmz.nrel.gov/apps/data_api.pl?site=UOSMRL&begin={yesterday_date}&end={yesterday_date}' 

response = requests.get(api_url)

if response.status_code == 200:
    
    csv_data = StringIO(response.text) 
    df = pd.read_csv(csv_data)

#DATETIME CALLS
df.rename(columns={'PST': 'Timezone', 'Relative Humidity [%]': 'Rel Humidity [%]'}, inplace=True)
df['DOY'] = df['DOY'].apply(add_leading_zeros)
df['YYYYDDD'] = (df['Year'].astype(str) + df['DOY'].astype(str)).astype(int)
df['YYYYMMDD'] = df['YYYYDDD'].apply(convert_YYYYDDD_to_YYYYMMDD)
df['Datetime'] = df.apply(combine_date_and_time, axis=1)

#DEW POINT CALL
df['Dew Point Temp [deg C]'] = df.apply(calculate_dew_point, axis=1)

#SITE NAME
df['Source'] = 'UO'

#COLUMN RENAME
df_finalUO = df[['Source','Datetime', 'Global CMP22 [W/m^2]', 'Direct CHP1 [W/m^2]', 'Diffuse CMP22 [W/m^2]', 'Zenith Angle [degrees]', 'Azimuth Angle [degrees]', 'Air Temperature [deg C]', 'Dew Point Temp [deg C]',
    'Rel Humidity [%]', 'Station Pressure [mBar]', 'Avg Wind Speed @ 10m [m/s]', 'Peak Wind Speed @ 10m [m/s]', 'Avg Wind Direction @ 10m [deg from N]', 'Logger Temp [deg C]', 'Logger Battery [VDC]']]
df_finalUO.columns = df_rename


###NREL SOLAR RADIATION RESEARCH LABORATORY (BMS)

api_url = f'https://midcdmz.nrel.gov/apps/data_api.pl?site=BMS&begin={yesterday_date}&end={yesterday_date}' 

response = requests.get(api_url)

if response.status_code == 200:
    
    csv_data = StringIO(response.text) 
    df = pd.read_csv(csv_data)

#DATETIME CALLS
df.rename(columns={'MST': 'Timezone', 'Tower RH [%]': 'Rel Humidity [%]'}, inplace=True)
df['DOY'] = df['DOY'].apply(add_leading_zeros)
df['YYYYDDD'] = (df['Year'].astype(str) + df['DOY'].astype(str)).astype(int)
df['YYYYMMDD'] = df['YYYYDDD'].apply(convert_YYYYDDD_to_YYYYMMDD)
df['Datetime'] = df.apply(combine_date_and_time, axis=1)

#KELVIN TO CELSIUS CALLS
df['CMP22 Case Temp [deg C]'] = pd.to_numeric(df['CMP22 Case Temp [deg K]'])
df['CMP22 Case Temp [deg C]'] = df['CMP22 Case Temp [deg K]'].apply(KtoC)

#SITE NAME
df['Source'] = 'NRELBMS'

#COLUMN RENAME
df_finalBMS = df[['Source','Datetime', 'Global CMP22-1 (cor) [W/m^2]', 'Direct CHP1-1 [W/m^2]', 'Diffuse CM22-1 [mV]', 'Zenith Angle [degrees]', 'Azimuth Angle [degrees]', 'Tower Dry Bulb Temp [deg C]', 
'Tower Dew Point Temp [deg C]', 'Rel Humidity [%]', 'Station Pressure [mBar]', 'Avg Wind Speed @ 6ft [m/s]', 'Peak Wind Speed @ 6ft [m/s]', 'Avg Wind Direction @ 6ft [deg from N]', 'CMP22 Case Temp [deg C]', 'CR3000 Deck Battery [VDC]']]
df_finalBMS.columns = df_rename


###SOLAR TECHNOLOGY ACCELERATION CENTER STAC  

api_url = f'https://midcdmz.nrel.gov/apps/data_api.pl?site=STAC&begin={yesterday_date}&end={yesterday_date}' 

response = requests.get(api_url)

if response.status_code == 200:
    
    csv_data = StringIO(response.text) 
    df = pd.read_csv(csv_data)

#DATETIME CALLS
df.rename(columns={'MST': 'Timezone'}, inplace=True)
df['DOY'] = df['DOY'].apply(add_leading_zeros)
df['YYYYDDD'] = (df['Year'].astype(str) + df['DOY'].astype(str)).astype(int)
df['YYYYMMDD'] = df['YYYYDDD'].apply(convert_YYYYDDD_to_YYYYMMDD)
df['Datetime'] = df.apply(combine_date_and_time, axis=1)

#SITE NAME
df['Source'] = 'STAC'

#COLUMN RENAME 
df_finalSTAC = df[['Source','Datetime', 'Global Horizontal [W/m^2]', 'Direct Normal [W/m^2]', 'Diffuse Horizontal [W/m^2]', 'Zenith Angle [degrees]', 'Azimuth Angle [degrees]', 'Air Temperature [deg C]', 'Dew Point Temp [deg C]',
    'Rel Humidity [%]', 'Station Pressure [mBar]', 'Avg Wind Speed @ 10m [m/s]', 'Peak Wind Speed @ 10m [m/s]', 'Avg Wind Direction @ 10m [deg from N]', 'CR1000 Temp [deg C]', 'CR1000 Battery [VDC]']]
df_finalSTAC.columns = df_rename


###UNIVERSITY OF FLORIDA

api_url = f'https://midcdmz.nrel.gov/apps/data_api.pl?site=UFL&begin={yesterday_date}&end={yesterday_date}' 

response = requests.get(api_url)

if response.status_code == 200:
    
    csv_data = StringIO(response.text) 
    df = pd.read_csv(csv_data)

#DATETIME CALLS
df.rename(columns={'EST': 'Timezone'}, inplace=True)
df['DOY'] = df['DOY'].apply(add_leading_zeros)
df['YYYYDDD'] = (df['Year'].astype(str) + df['DOY'].astype(str)).astype(int)
df['YYYYMMDD'] = df['YYYYDDD'].apply(convert_YYYYDDD_to_YYYYMMDD)
df['Datetime'] = df.apply(combine_date_and_time, axis=1)


#SITE NAME
df['Source'] = 'UFL'

#COLUMN RENAME
df_finalUFL = df[['Source','Datetime', 'Global Horiz [W/m^2]', 'Direct Normal [W/m^2]', 'Diffuse Horiz [W/m^2]', 'Zenith Angle [degrees]', 'Azimuth Angle [degrees]', 'Air Temperature [deg C]', 'Dew Point Temp [deg C]',
    'Rel Humidity [%]', 'Station Pressure [mBar]', 'Avg Wind Speed @ 3m [m/s]', 'Peak Wind Speed @ 3m [m/s]', 'Avg Wind Direction @ 3m [deg from N]', 'Logger Temp [deg C]', 'Logger Battery [VDC]']]
df_finalUFL.columns = df_rename




###########FINAL INSERT INTO SQL SERVER DATABASE 'master'

#INSERT INTO SOLAR ENVIRONMENT TABLE
table_name = 'slr_envrnmnt_stage'

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# INSERT DATAFRAME INTO SQL TABLE
insert_query = f'INSERT INTO {table_name} (Source, Datetime, [GlobalHoriz(W/m^2)], [DirectNormal(W/m^2)], [DiffuseHoriz(W/m^2)], [ZenithAngle(degrees)], 
[AzimuthAngle(degrees)], [AirTemperature(degC)], [DewPointTemp(degC)], [RelHumidity(%)], [StationPressure(mBar)], [AvgWindSpeed(m/s)], [PeakWindSpeed(m/s)], [AvgWindDirection(degfromN)], [LoggerTemp(degC)], 
 [LoggerBattery(VDC)]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

try:
    for index, row in df_finalUAOASIS.iterrows():
        cursor.execute(insert_query, row['Source'], row['Datetime'], row['Global Horiz [W/m^2]'], row['Direct Normal [W/m^2]'], row['Diffuse Horiz [W/m^2]'], row['Zenith Angle [degrees]'], row['Azimuth Angle [degrees]']
                       , row['Air Temperature [deg C]'], row['Dew Point Temp [deg C]'], row['Rel Humidity [%]'], row['Station Pressure [mBar]'], row['Avg Wind Speed [m/s]'], row['Peak Wind Speed [m/s]'], row['Avg Wind Direction [deg from N]']
                       , row['Logger Temp [deg C]'], row['Logger Battery [VDC]'])
        
    for index, row in df_finalUO.iterrows():
        cursor.execute(insert_query, row['Source'], row['Datetime'], row['Global Horiz [W/m^2]'], row['Direct Normal [W/m^2]'], row['Diffuse Horiz [W/m^2]'], row['Zenith Angle [degrees]'], row['Azimuth Angle [degrees]']
                       , row['Air Temperature [deg C]'], row['Dew Point Temp [deg C]'], row['Rel Humidity [%]'], row['Station Pressure [mBar]'], row['Avg Wind Speed [m/s]'], row['Peak Wind Speed [m/s]'], row['Avg Wind Direction [deg from N]']
                       , row['Logger Temp [deg C]'], row['Logger Battery [VDC]'])
        
    for index, row in df_finalBMS.iterrows():
        cursor.execute(insert_query, row['Source'], row['Datetime'], row['Global Horiz [W/m^2]'], row['Direct Normal [W/m^2]'], row['Diffuse Horiz [W/m^2]'], row['Zenith Angle [degrees]'], row['Azimuth Angle [degrees]']
                       , row['Air Temperature [deg C]'], row['Dew Point Temp [deg C]'], row['Rel Humidity [%]'], row['Station Pressure [mBar]'], row['Avg Wind Speed [m/s]'], row['Peak Wind Speed [m/s]'], row['Avg Wind Direction [deg from N]']
                       , row['Logger Temp [deg C]'], row['Logger Battery [VDC]'])
        
    for index, row in df_finalSTAC.iterrows():
        cursor.execute(insert_query, row['Source'], row['Datetime'], row['Global Horiz [W/m^2]'], row['Direct Normal [W/m^2]'], row['Diffuse Horiz [W/m^2]'], row['Zenith Angle [degrees]'], row['Azimuth Angle [degrees]']
                       , row['Air Temperature [deg C]'], row['Dew Point Temp [deg C]'], row['Rel Humidity [%]'], row['Station Pressure [mBar]'], row['Avg Wind Speed [m/s]'], row['Peak Wind Speed [m/s]'], row['Avg Wind Direction [deg from N]']
                       , row['Logger Temp [deg C]'], row['Logger Battery [VDC]'])
        
    for index, row in df_finalUFL.iterrows():
        cursor.execute(insert_query, row['Source'], row['Datetime'], row['Global Horiz [W/m^2]'], row['Direct Normal [W/m^2]'], row['Diffuse Horiz [W/m^2]'], row['Zenith Angle [degrees]'], row['Azimuth Angle [degrees]']
                       , row['Air Temperature [deg C]'], row['Dew Point Temp [deg C]'], row['Rel Humidity [%]'], row['Station Pressure [mBar]'], row['Avg Wind Speed [m/s]'], row['Peak Wind Speed [m/s]'], row['Avg Wind Direction [deg from N]']
                       , row['Logger Temp [deg C]'], row['Logger Battery [VDC]'])

    conn.commit()
    print("Data inserted successfully.")

except Exception as e:
    print("Error occurred:", e)
    conn.rollback()  # ROLLBACK IF ERROR

finally:
    cursor.close()
    conn.close()




