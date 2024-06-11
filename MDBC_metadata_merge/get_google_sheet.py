import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

def get_sheet(creds:Credentials, sheet_id:str, range:str)->pd.DataFrame:
  try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Drive v3 API
    table = service.spreadsheets()
    results = (
        table.values()
        #.list(pageSize=50, fields="nextPageToken, files(id, name)")
        .get(spreadsheetId = sheet_id, range = range)
        .execute()
    )
    if not results:
      print("No files found.")
      return

    items = results.get('values', [])
    dataframe = pd.DataFrame(items[1:], columns = items[0])
    
    #items = results.get("values", [])
    
    return dataframe

    
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")
    return []
