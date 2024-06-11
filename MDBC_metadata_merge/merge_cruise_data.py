#Import necessary libraries and connect to your AGOL account
import os
import pandas as pd
import pyodbc
import numpy as np
from IPython.display import display

import geopandas as gpd
from shapely.geometry import Point, LineString

from pyodbc import Connection
from google_auth_scopeagnostic import get_credentials
from get_google_sheet import get_sheet
from GIS_access import update_gis_online
from get_azure_secret import get_secret

#Specify SQL Server DB connection strings
def get_connection(config:str)->Connection:
    #replace this with parsing info from a config
    cnxn_str = get_secret("nccos-development-sqlserver-nccosmdbcdev-asmita")
    cnxn = pyodbc.connect(cnxn_str)
    return cnxn

def get_google_info(google_config:str = None)->list:
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    sheet_id = '1qW0mmq6FPPUzcTsigrhlCeaxfCjdcdoXCCgEtszr7XA'
    target_range = "Video!A:Y"
    return scope, sheet_id, target_range


def query_and_merge_metadata():
    #region query
    #Get cruise metadata from CRUISEINFO table

    cnxn = get_connection(None)
    CruiseQuery = """SELECT CruiseID, CruiseName, CalendarYr, FieldSeason FROM [nccosmdbcdev].[dbo].[CRUISEINFO]"""
    CruiseInfo = pd.read_sql(CruiseQuery, cnxn)

    #Get ROV Navigation data
    NavQuery = """SELECT CruiseID, DiveID, TmStamp_UTC,Latitude, Longitude, Depth_m, QualityCode FROM [nccosmdbcdev].[dbo].[ROVNAV3] WHERE BottomFlag = 'Y'"""
    NavData = pd.read_sql(NavQuery, cnxn)
    #NavQuery = """SELECT * FROM [nccosmdbcdev].[dbo].[ROVNAV3] where LastUpdated >= DATEADD(day, -7, GETDATE())"""

    #Get dive metadata from DIVEVEHICLEINFO table
    DiveQuery = """SELECT CruiseID, DiveID, CTDCollected, DiveDuration FROM [nccosmdbcdev].[dbo].[DIVEVEHICLEINFO]"""
    DiveMetadata = pd.read_sql(DiveQuery, cnxn)


    cnxn.close() #Close connection to SQL Server Database

    #endregion

    #region transform/merge
    #massage nav data
    gdf = gpd.GeoDataFrame(NavData)
    gdf.set_geometry(gpd.points_from_xy(gdf['Longitude'], gdf['Latitude']), inplace=True, crs='EPSG:4326') 
    aggregations = {'Depth_m': np.nanmax,
                'geometry': lambda x: LineString(x.tolist()),
                    'QualityCode': max}
    gdf1 = gdf.sort_values(by=['TmStamp_UTC']).groupby(['CruiseID','DiveID'], as_index=False).agg(aggregations)

    #
    gdf1 = gpd.GeoDataFrame(gdf1, geometry='geometry')
    gdf1 = gdf1.set_crs("EPSG:4326")
    gdf1['Quality'] = np.where(gdf1['QualityCode']==2,"Good Quality Smooth Track", "Unchecked Raw Data")
    gdf1 = gdf1.drop(columns=['QualityCode'])
    gdf1.rename(columns={'Depth_m': 'Max_Dive_Depth_m'}, inplace=True)
    #print(gdf1)

    #merge nav with dive
    gdf1 = gdf1.merge(DiveMetadata, how='left', on=['CruiseID','DiveID'])
    gdf1 = gdf1.merge(CruiseInfo, how='left', on=['CruiseID'])
    gdf1['CalendarYr'] = gdf1['CalendarYr'].astype('int', copy=False)
    gdf1['DiveDuration'] = gdf1['DiveDuration'].fillna(0)
    gdf1['DiveDuration'] = gdf1['DiveDuration'].replace(',','').astype('int')
    gdf1 = gdf1[['CruiseID','CruiseName','CalendarYr','FieldSeason','DiveID','CTDCollected','Max_Dive_Depth_m','DiveDuration','Quality','geometry']]

    #endregion

    #region google_sheet query
    ###Then I need to be able to pull in some more data from a Google Sheet but since I can't do it yet, I download the sheet everytime and then do a merge#####
    #create and use credentials
    scope, sheet_id, target_range = get_google_info()
    MyCreds = get_credentials(scope)
    AnnotationStatus = get_sheet(creds=MyCreds, sheet_id = sheet_id, range = target_range)

    #endregion

    gdf1 = gdf1.merge(AnnotationStatus, how='left', on=['CruiseID','DiveID'])

    #region GIS access
    ###Create a spatially enabled dataframe based on the geometry column and publish the hosted feature layer
    secret_un = get_secret("arcgis-online-username")
    secret_pw = get_secret("arcgis-online-pw")
    creds = [secret_un, secret_pw]
    update_gis_online(gdf1, creds)
    #endregion

if __name__=="__main__":
    query_and_merge_metadata()