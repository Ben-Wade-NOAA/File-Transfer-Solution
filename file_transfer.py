import os
from azure.storage.blob import BlobClient, ContainerClient, BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient




class FileTransferClient:
    def __init__(self,
                 account_url:str = None, 
                 contianer_name:str = None, 
                 file_destination:str = './'):
        self.__account_url = account_url
        self.__container_name = contianer_name
        self.__data_folder_path = file_destination
        try:
            self.__creds = DefaultAzureCredential()
        except:
            print("Error establishing credentials. Exiting.")
            exit(1)
        
        try:
            self.__ml_client = MLClient.from_config(credential=self.__creds)
        except:
            print("Error establishing ML Client Obejct. Exiting.")
            exit(1)
        
        try:
            self.__blob_client = BlobClient(container_name=self.__container_name, credential=self.__creds)
        except:
            print("Error Establishing Blob Client. Exiting.")
            exit(1)
        
        try:
            self.__blob_service_client = BlobServiceClient(account_url = self.__account_url, credential=self.__creds)
        except:
            print("Error establishing Blob Service Client. Exiting.")
            exit(1)
        
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
    
def upload_folder_contents_to_blob(account_url:str = None, container_name:str = None, source_folder:str = None):
    pass

transfer_from_blob_to_compute(account_url='https://nccoswsdevstor.blob.core.windows.net',
                              container_name = 'azureml-blobstore-3135c7f3-1c7c-41b3-bf87-1a99aa567722',
                              regex_key = 'UTC/DSC',
                              file_destination='./Data')