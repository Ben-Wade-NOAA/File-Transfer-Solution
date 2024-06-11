import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from arcgis.gis import GIS

def update_gis_online(updated_cruise_info:pd.DataFrame, login_creds:list):
    gis = GIS("home")
    sedf=GeoAccessor.from_geodataframe(updated_cruise_info, column_name="geometry")
    ROVTracks = sedf.spatial.to_featurelayer("MDBC: ROV Dive Tracks") #specify the layer title here