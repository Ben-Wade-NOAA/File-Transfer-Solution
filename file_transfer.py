import os
import psutil
from sys import exit
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
        self.__target_blobs = []
        self.__to_gb = 1024.0**3
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
        
        #try:
        #    self.__blob_client = BlobClient(container_name=self.__container_name, credential=self.__creds)
        #except:
        #    print("Error Establishing Blob Client. Exiting.")
        #    exit(1)
        
        try:
            self.__blob_service_client = BlobServiceClient(account_url = self.__account_url, credential=self.__creds)
        except:
            print("Error establishing Blob Service Client. Exiting.")
            exit(1)
        
        self.__get_available_disk
        self.__get_available_memory
    
    def __get_available_disk(self):
        
        
        self.__total_disk = 0.0
        self.__used_disk = 0.0
        self.__free_disk = 0.0
        self.__percent = 0.0
        try:
            self.__total_disk, self.__used_disk, self.__free_disk, self.__percent = psutil.disk_usage('/')
        except:
            print("Somehow, the script doesn't have read access to disk. I don't know how we got here...")
            exit(1)
            
        self.__total_disk /= self.__to_gb
        self.__used_disk /= self.__to_gb
        self.__free_disk /= self.__to_gb
        
       
    def __get_available_memory(self):
        try:
            memory_stats = psutil.virtual_memory()
        except:
            print("Somehow, the script can't access the physical memory. Check the logs")
            exit(1)
        
        
        
        self.__total_mem = memory_stats[0]/self.__to_gb
        self.__available_mem = memory_stats[1]/self.__to_gb
        self.__used_mem = memory_stats[3]/self.__to_gb
        self.__free_mem = memory_stats[4]/self.__to_gb

    
    def __get_container_size(self, regex_key:str = None):
        size = 0.0
        blob_service_client = self.__blob_service_client
        container_client = blob_service_client.get_container_client(self.__container_name)
        
        return size
    
    def __get_target_blob_list(self, regex_key:str = None, container_client:ContainerClient = None):
        key = regex_key
  
        for name in container_client.list_blob_names():
            if key in name:
                self.__target_blobs.append(name)
            else:
                pass   
        
        
    
    def transfer_from_blob_to_compute(self, regex_key:str = None):
        
        key = regex_key
               
        creds = self.__creds
        
        blob_service_client = self.__blob_service_client
        container_client = blob_service_client.get_container_client(self.__container_name)
        
        self.__get_target_blob_list(self, regex_key= key, container_client= container_client)

        if(len(self.__target_blobs)!=0):
            try:
                for blob_name in self.__target_blobs:
                    cleaned_blob_name = blob_name.split('/')[-1]
                    with open(file = os.path.join(self.__data_folder_path, cleaned_blob_name), mode = 'wb') as download_file:
                        download_file.write(container_client.download_blob(blob_name).readall())
            except:
                print("Something went wrong with the file transfer from {self.__container_name} to {self.__data_folder_path}. Please check the logs and try again")
        else:
            print("No blobs found containing the characters: {regex_key}")
            
            
    def upload_folder_to_blob(self, target_folder:str = None):
        if(target_folder == None):
            print("No target folder found. Using folder from object instantiation")
            target_folder = self.__data_folder_path
        else:
            target_folder = target_folder
        local_blob_client = self.__blob_service_client
        container_client = local_blob_client.get_container_client(container=self.__container_name)
        #get files in whatever directory you're trying to upload
        local_file_list = os.listdir(target_folder)
        #strip out the files that were downloaded to begin with
        local_file_list = [file_name for file_name in local_file_list if file_name not in self.__target_blobs]  #I hate this line of code but it's otherwise really inefficient      
        #upload the files that are left using the container client
        

        pass


'''
transfer_from_blob_to_compute(account_url='https://nccoswsdevstor.blob.core.windows.net',
                              container_name = 'azureml-blobstore-3135c7f3-1c7c-41b3-bf87-1a99aa567722',
                              regex_key = 'UTC/DSC',
                              file_destination='./Data')
'''