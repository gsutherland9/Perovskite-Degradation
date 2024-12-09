
#####LOAD IN SITE DATA FROM SQL SERVER, ANALYZE VARIANCES FOR SIGNIFICANCE, AND PLOT 2024 FOR ALL METRICS

import pandas as pd
import numpy as np
import pyodbc
import time
import seaborn as sns
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.multivariate.manova import MANOVA
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd

##SQL QUERY
def sql_server_query():

    query_str = '''
                SELECT s.Source
                    ,s.DATE
                    ,a.[RelHumidity(%)]
                    ,b.[AirTemperature(degC)]
                    ,c.[GlobalHoriz(W/m^2)]
                    ,d.[DailyTempFluctuation(degC)]
                    ,e.[StationPressure(mBar)]
                FROM (
                    SELECT Source
                        ,cast(DATETIME AS DATE) DATE
                    FROM master.dbo.slr_envrnmnt
                    WHERE [RelHumidity(%)] >= 0
                    GROUP BY source
                        ,cast(DATETIME AS DATE)
                    ) s
                LEFT JOIN (
                    SELECT Source
                        ,cast(DATETIME AS DATE) DATE
                        ,avg([RelHumidity(%)]) [RelHumidity(%)]
                    FROM master.dbo.slr_envrnmnt
                    WHERE [RelHumidity(%)] >= 0
                    GROUP BY Source
                        ,cast(DATETIME AS DATE)
                    ) a ON a.source = s.source
                    AND a.DATE = s.DATE
                LEFT JOIN (
                    SELECT Source
                        ,cast(DATETIME AS DATE) DATE
                        ,avg([AirTemperature(degC)]) [AirTemperature(degC)]
                    FROM master.dbo.slr_envrnmnt
                    WHERE [AirTemperature(degC)] > - 50
                    GROUP BY Source
                        ,cast(DATETIME AS DATE)
                    ) b ON b.Source = s.Source
                    AND b.DATE = s.DATE
                LEFT JOIN (
                    SELECT Source
                        ,cast(DATETIME AS DATE) DATE
                        ,avg([GlobalHoriz(W/m^2)]) [GlobalHoriz(W/m^2)]
                    FROM (
                        SELECT Source
                            ,DATETIME
                            ,CASE 
                                WHEN Source = 'UFL'
                                    AND cast(DATETIME AS DATE) BETWEEN '2024-01-01'
                                        AND '2024-01-31'
                                    THEN NULL
                                ELSE [GlobalHoriz(W/m^2)]
                                END AS [GlobalHoriz(W/m^2)]
                        FROM master.dbo.slr_envrnmnt
                        ) i
                    WHERE [GlobalHoriz(W/m^2)] BETWEEN - 10
                            AND 1500
                    GROUP BY Source
                        ,cast(DATETIME AS DATE)
                    ) c ON c.Source = s.Source
                    AND C.DATE = s.DATE
                LEFT JOIN (
                    SELECT Source
                        ,avg([DailyTempFluctuation(degC)]) [DailyTempFluctuation(degC)]
                        ,DATE
                    FROM (
                        SELECT Source
                            ,cast(DATETIME AS DATE) DATE
                            ,(max([AirTemperature(degC)]) - min([AirTemperature(degC)])) [DailyTempFluctuation(degC)]
                        FROM master.dbo.slr_envrnmnt
                        WHERE [AirTemperature(degC)] > - 50
                        GROUP BY Source
                            ,cast(DATETIME AS DATE)
                        ) g
                    GROUP BY Source
                        ,DATE
                    ) d ON d.Source = s.Source
                    AND d.DATE = s.DATE
                LEFT JOIN (
                    SELECT Source
                        ,cast(DATETIME AS DATE) DATE
                        ,avg([StationPressure(mBar)]) [StationPressure(mBar)]
                    FROM master.dbo.slr_envrnmnt
                    WHERE [StationPressure(mBar)] > - 500
                    GROUP BY Source
                        ,cast(DATETIME AS DATE)
                    ) e ON e.Source = s.Source
                    AND e.DATE = s.DATE
            ''' 

    return query_str


def sql_server_connect():
    #server = #####
    #database = #####
    #username =  #####
    #driver = #####

    try:
        conn = pyodbc.connect(f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes")

        query_str = sql_server_query()

        query_df = pd.read_sql_query(query_str, conn)

        return query_df

    except pyodbc.error as err:
        raise

    finally:
        conn.close


df = sql_server_connect()
df

###########################
#######ANALYSES OF VARIANCE

#RENAME COLUMNS
df.columns = ['Source', 'Date', 'RelHumidity', 'AirTemperature', 'GlobalHoriz', 'DailyTempFluctuation', 'StationPressure']

#MANOVA FORMULA
formula = 'RelHumidity + AirTemperature + GlobalHoriz + DailyTempFluctuation + StationPressure ~ Source'

#FIT MANOVA
manova = MANOVA.from_formula(formula, data=df)

result = manova.mv_test()
print(result)


#TUKEY'S TESTS
tukey_result_humidity = pairwise_tukeyhsd(df['RelHumidity'], df['Source'])
print(tukey_result_humidity)

tukey_result_airtemp = pairwise_tukeyhsd(df['AirTemperature'], df['Source'])
print(tukey_result_airtemp)

##CORRECTION FOR NULL UFL ROWS
df_glblhrz = df[df['GlobalHoriz'].notna()]

tukey_result_GlobalHoriz = pairwise_tukeyhsd(df_glblhrz['GlobalHoriz'], df_glblhrz['Source'])
print(tukey_result_GlobalHoriz)

tukey_result_DailyTempFluctuation = pairwise_tukeyhsd(df['DailyTempFluctuation'], df['Source'])
print(tukey_result_DailyTempFluctuation)

tukey_result_StationPressure = pairwise_tukeyhsd(df['StationPressure'], df['Source'])
print(tukey_result_StationPressure)



########################################
###########################GRAPH OUTPUTS

#RELATIVE HUMIDITY OUTPUT
df_relhumidity = df[['Source', 'Date', 'RelHumidity']]
df_relhumidity['Date'] = pd.to_datetime(df_relhumidity['Date'])
df_relhumidity['Month'] = df_relhumidity['Date'].dt.month
monthly_avg_relhumidity = df_relhumidity.groupby(['Month','Source'])['RelHumidity'].mean().reset_index()

plt.figure(figsize=(13, 6))
sns.lineplot(data=monthly_avg_relhumidity, x='Month', y='RelHumidity', hue='Source', marker='x')

plt.title('2024 Average Daily Relative Humidity by Month')
plt.xlabel('Month')
plt.ylabel('Relative Humidity (%)')
plt.legend(title='Site')
plt.grid(False)
plt.xticks(ticks=range(1, 13), labels=range(1, 13))

plt.show()


#AVERAGE TEMPERATURE OUTPUT
df_avgtemperature = df[['Source', 'Date', 'AirTemperature']]
df_avgtemperature['Date'] = pd.to_datetime(df_avgtemperature['Date'])
df_avgtemperature['Month'] = df_avgtemperature['Date'].dt.month
monthly_avgtemperature = df_avgtemperature.groupby(['Month','Source'])['AirTemperature'].mean().reset_index()

plt.figure(figsize=(13, 6))
sns.lineplot(data=monthly_avgtemperature, x='Month', y='AirTemperature', hue='Source', marker='x')

plt.title('2024 Average Daily Air Temperature by Month')
plt.xlabel('Month')
plt.ylabel('Air Temperature (deg C)')
plt.legend(title='Site')
plt.grid(False)
plt.xticks(ticks=range(1, 13), labels=range(1, 13))

plt.show()


#AVERAGE TEMPERATURE FLUCTUATION OUTPUTS
df_avgtemperatureflux = df[['Source', 'Date', 'DailyTempFluctuation']]
df_avgtemperatureflux['Date'] = pd.to_datetime(df_avgtemperatureflux['Date'])
df_avgtemperatureflux['Month'] = df_avgtemperatureflux['Date'].dt.month
monthly_avgtemperatureflux = df_avgtemperatureflux.groupby(['Month','Source'])['DailyTempFluctuation'].mean().reset_index()

plt.figure(figsize=(13, 6))
sns.lineplot(data=monthly_avgtemperatureflux, x='Month', y='DailyTempFluctuation', hue='Source', marker='x')

plt.title('2024 Average Daily Air Temperature Fluctuation by Month')
plt.xlabel('Month')
plt.ylabel('Air Temperature Fluctuation (deg C)')
plt.legend(title='Site')
plt.grid(False)
plt.xticks(ticks=range(1, 13), labels=range(1, 13))

plt.show()


#AVERAGE GLOBAL HORIZONTAL IRRADIANCE OUTPUTS
df_avgglobalhoriz = df[['Source', 'Date', 'GlobalHoriz']]
df_avgglobalhoriz['Date'] = pd.to_datetime(df_avgglobalhoriz['Date'])
df_avgglobalhoriz['Month'] = df_avgglobalhoriz['Date'].dt.month
monthly_avgglobalhoriz = df_avgglobalhoriz.groupby(['Month','Source'])['GlobalHoriz'].mean().reset_index()

plt.figure(figsize=(13, 6))
sns.lineplot(data=monthly_avgglobalhoriz, x='Month', y='GlobalHoriz', hue='Source', marker='x')

plt.title('2024 Average Daily Global Horizontal Irradiance by Month')
plt.xlabel('Month')
plt.ylabel('Global Horizontal Irradiance (W/m^2)')
plt.legend(title='Site')
plt.grid(False)
plt.xticks(ticks=range(1, 13), labels=range(1, 13))

plt.show()


#AVERAGE AIR PRESSURE OUTPUTS
df_avgstationpressure = df[['Source', 'Date', 'StationPressure']]
df_avgstationpressure['Date'] = pd.to_datetime(df_avgstationpressure['Date'])
df_avgstationpressure['Month'] = df_avgstationpressure['Date'].dt.month
monthly_avgstationpressure = df_avgstationpressure.groupby(['Month','Source'])['StationPressure'].mean().reset_index()

plt.figure(figsize=(13, 6))
sns.lineplot(data=monthly_avgstationpressure, x='Month', y='StationPressure', hue='Source', marker='x')

plt.title('2024 Average Daily Station Pressure by Month')
plt.xlabel('Month')
plt.ylabel('Station Pressure (mbar)')
plt.legend(title='Site')
plt.grid(False)
plt.xticks(ticks=range(1, 13), labels=range(1, 13))

plt.show()