import os
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient


def transfer_from_blob_to_compute(account_url:str = None, container_name:str = None, regex_key:str = None, file_destination:str = './'):
    account_url = account_url
    container_name = container_name
    key = regex_key
       
    
    creds = DefaultAzureCredential()
    ml_client = MLClient.from_config(credential = creds)
    blob_service_client = BlobServiceClient(account_url=account_url, credential=creds)
    container_client = blob_service_client.get_container_client(container_name)
    
    needed_blobs = []
    for name in container_client.list_blob_names():
        if key in name:
            needed_blobs.append(name)
        else:
            pass   
    
    for blob_name in needed_blobs:
        cleaned_blob_name = blob_name.split('/')[-1]
        with open(file = os.path.join(file_destination, cleaned_blob_name), mode = 'wb') as download_file:
            download_file.write(container_client.download_blob(blob_name).readall())
    


transfer_from_blob_to_compute(account_url='https://nccoswsdevstor.blob.core.windows.net',
                              container_name = 'azureml-blobstore-3135c7f3-1c7c-41b3-bf87-1a99aa567722',
                              regex_key = 'UTC/DSC',
                              file_destination='./Data')