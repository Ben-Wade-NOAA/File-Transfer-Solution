# You can import the get_credentials function from google_auth.py and use it to obtain the credentials:
# from google_auth import get_credentials
# credentials = get_credentials()
# Now you can use 'credentials' in your Google API calls

import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

#  *** Pay attention to this ===>>> If modifying these scopes, delete the file token.json.


def get_credentials(SCOPES:list)->Credentials:
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

