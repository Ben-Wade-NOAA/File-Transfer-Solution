import os
import psutil
from sys import exit
from azure.storage.blob import BlobClient, ContainerClient, BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.ai.ml import MLClient




class FileTransferClient:
    def __init__(self,
                 account_url:str = None, 
                 container_name:str = None, 
                 local_folder:str = './',
                 cloud_folder:str = None):
        self.__account_url = account_url
        self.__container_name = container_name
        self.__local_folder_path = local_folder
        if not os.path.exists(self.__local_folder_path):
            print("Target directory does not yet exist. Making target directory")
            os.mkdir(self.__local_folder_path)
        self.__cloud_folder_path = cloud_folder
        
        self.__to_gb = 1024.0**3
        try:
            self.__creds = AzureCliCredential()
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
        self.__container_client = self.__blob_service_client.get_container_client(self.__container_name)
        self.__target_blobs = []
        
        
        self.__get_target_blob_list(key = self.__cloud_folder_path)
        self.__get_available_disk()
        self.__get_available_memory()
    
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

        for datum in memory_stats:
            print(datum)
    
    def __get_container_size(self, regex_key:str = None):
        size = 0.0
        blob_service_client = self.__blob_service_client
        container_client = blob_service_client.get_container_client(self.__container_name)
        
        return size
    
    def __get_target_blob_list(self, key:str = None):
        if self.__container_client:
            container_client = self.__container_client
            try:
                for name in container_client.list_blob_names():
                    if (key in name) and not ('.aml' in name):
                        self.__target_blobs.append(name)
                    else:
                        pass   
            except Exception as e:
                print(e)
                print('Sorry, no blobs found with that folder path in the chosen container')
        else:
            print("Somehow there is no container client active in the file transfer client")
    
    def __strip_system_files(self, file_list)->list: 
        for file in file_list:
            print(file[0], print(file), print(file[0]=='.'))
            if file[0]=='.':
                file_list.remove(file)
        return file_list
    
    def transfer_from_blob_to_compute(self):
         
        blob_service_client = self.__blob_service_client
        container_client = blob_service_client.get_container_client(self.__container_name)
               

        if(len(self.__target_blobs)!=0):
            try:
                for blob_name in self.__target_blobs:
                    cleaned_blob_name = blob_name.split('/')[-1]
                    cleaned_blob_name = self.__strip_system_files(cleaned_blob_name)
                    with open(file = os.path.join(self.__data_folder_path, cleaned_blob_name), mode = 'wb') as download_file:
                        download_file.write(container_client.download_blob(blob_name).readall())
            except Exception as e:
                print(e)
                print("Something went wrong with the file transfer from {} to {}. Please check the logs and try again".format(self.__container_name, self.__data_folder_path))
        else:
            print("No blobs found containing the characters: {}".format(self.__cloud_folder_path))
            
            
    def upload_folder_to_blob(self, source_folder:str, destination_folder:str = None):
        if(destination_folder == None):
            print("No destination folder found. Using folder from object instantiation")
            destination_folder = self.__cloud_folder_path
        else:
            destination_folder = destination_folder
        local_blob_client = self.__blob_service_client
        container_client = local_blob_client.get_container_client(container=self.__container_name)
        #get files in whatever directory you're trying to upload
        local_file_list = os.listdir(source_folder)
        #strip out the files that were downloaded to begin with
        local_file_list = [file_name for file_name in local_file_list if (file_name not in self.__target_blobs) and not ('.aml' in file_name)]  #I hate this line of code but it's otherwise really inefficient      
        #upload the files that are left using the container client
        print(source_folder)
        print(local_file_list)
        
       # local_file_list = self.__strip_system_files(local_file_list)
        
        for upload_file in local_file_list:
            
            with open(file = os.path.join(self.__local_folder_path, upload_file), mode = 'rb') as data:
                print(os.path.join(destination_folder, upload_file))
                blob_client = container_client.upload_blob(name = os.path.join(destination_folder, upload_file), data = data, overwrite = False)
        pass


'''
transfer_from_blob_to_compute(account_url='https://nccoswsdevstor.blob.core.windows.net',
                              container_name = 'azureml-blobstore-3135c7f3-1c7c-41b3-bf87-1a99aa567722',
                              regex_key = 'UTC/DSC',
                              file_destination='./Data')
'''