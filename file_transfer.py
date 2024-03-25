'''
author: Ben Wade
email: ben.wade@noaa.gov
If I pooched it, let me know
'''

import os
import psutil
from sys import exit
from azure.storage.blob import BlobClient, ContainerClient, BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.ai.ml import MLClient




class FileTransferClient:
    #region init
    def __init__(self,
                 account_url:str = None, 
                 container_name:str = None, 
                 local_folder:str = './',
                 cloud_folder:str = None
                 ):
        """Account URL: https://<your storage account>.<'file' or 'blob' depending on type>.core.windows.net \n
           Container Name: Name of the container who's parent directory is the storage account in azure storage explorer\n
           Local Folder: Which folder you want to put these files in. Will be the root folder and cloud structure will be preserved\n
           Cloud Folder: Where the name of the folder you want to copy down from the cloud. All child folders will be copied with it\n"""
        #establish the local container variables
        self.__account_url = account_url
        self.__container_name = container_name
        self.__local_folder_path = local_folder
        
        #make the destination folder
        if not os.path.exists(self.__local_folder_path):
            print("Target directory does not yet exist. Making target directory")
            os.mkdir(self.__local_folder_path)
        
        self.__cloud_folder_path = cloud_folder
        self.__to_gb = 1024.0**3
        
        #try to create login credentials
        try:
            self.__creds = AzureCliCredential()
        except Exception as e:
            print("The system encountered the following error: {} \n Error establishing credentials. Exiting.".format(e))
            exit(1)
        
        #try to create an as-of-yet unused ML Client
        try:
            self.__ml_client = MLClient.from_config(credential=self.__creds)
        except Exception as e:
            
            print("The system encountered the following error: {} \n Error establishing ML Client Obejct. Exiting.".format(e))
            exit(1)
        

        #try to make a blob client
        try:
            self.__blob_service_client = BlobServiceClient(account_url = self.__account_url, credential=self.__creds)
        except Exception as e:
            print("The system encountered the following error: {} \n Error establishing Blob Service Client. Exiting.".format(e))
            exit(1)
        #try to make a container client
        try:
            self.__container_client = self.__blob_service_client.get_container_client(self.__container_name)
        except Exception as e:
            print("The system encountered the following error: {} \n Error establishing Container Client, could not find or create the sepcified container".format(e))
            exit(1)
            
        self.__target_blobs = []
        
        self.__get_target_blob_list(key = self.__cloud_folder_path)
        

        self.__container_size = self.__get_container_size()
        self.__get_available_disk()
        self.__get_available_memory()
        
        print("### This file transfer method uses Azure CLI Credentials. Please type 'az login' into the terminal to authenticate those credentials ### \n")
        print("\n This method will allow you to download a whole folder in a blob container and upload completed products to a folder in the same container. \n")
        print("\n This object assumes that any file you have processed before uploading has had its name changed or exists in a different directory than its raw source data \n")
        print("\n If you need to upload or download to a different container, you'll need to make another client and handle those methods there by exchanging file paths.\n")
        print("\n If you find any logical errors or bugs, please email the author listed in the source code \n")
        #endregion 

    #region testing functions            
    #helper function to get blob names and folder structure
    def __print_blob_names(self):
        if(self.__target_blobs):
            for x in range(0, len(self.__target_blobs)):
                print(self.__target_blobs[x])
                
    def __try_copy(self):
        self.__copy_folder_structure()
    #endregion
    
    #region size functions
    #gets the available disk on local compute
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
    
    #gets available ram from the OS on the local compute
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

        
    #reads the size of the items to be downloaded from the cloud
    def __get_container_size(self)->float:
        size = 0.0
        container_client = self.__container_client
        for blob in self.__target_blobs:
            blob_client = container_client.get_blob_client(blob)
            size+= blob_client.get_blob_properties().size
        
        print('download size in bytes is {}'.format(size))
        return size/self.__to_gb
    
    #endregion
    
    #region file handling functions
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
    

    #takes the folder structure of the target blobs and copies it to the local compute cluster    
    def __copy_folder_structure(self):
        if len(self.__target_blobs)>0:
            working_list = self.__target_blobs
            num_items = len(working_list)
                       
            for x in range(0, num_items):
                current_file = working_list[x].split('/')
                current_file.pop()
                listed_path = os.path.join(self.__local_folder_path, *current_file)
                if not os.path.exists(listed_path):
                    os.makedirs(listed_path)
                    
    #tries to trip out the .aml files from upload, but doesn't work for some reasion
    def __strip_system_files(self, file_list)->list: 
        for file in file_list:
            print(file[0], print(file), print(file[0]=='.'))
            if file[0]=='.':
                file_list.remove(file)
        return file_list
    #endregion
    
    #region transfer functions
    def transfer_from_blob_to_compute(self):
        """Will transfer a folder and all its contents from a blob into the folder you specified when you created the object.\n
        The folder structure will be preserved, but currently cannot copy files from a folders that do not share the same parent directory."""
        blob_service_client = self.__blob_service_client
        container_client = blob_service_client.get_container_client(self.__container_name)
               
        self.__get_available_disk()
        self.__get_available_memory()
        
        if(len(self.__target_blobs)!=0)and(self.__free_disk>self.__container_size):#check to see if there's enough room on the local compute
            try:
                self.__copy_folder_structure()#copies to the folder structure
                for blob_name in self.__target_blobs:#loops through each blob
                    cleaned_blob_name = blob_name.split('/')#probably doesn't need to happen, but splits up by folders
                    
                    with open(file = os.path.join(self.__local_folder_path, *cleaned_blob_name), mode = 'wb') as download_file:
                        download_file.write(container_client.download_blob(blob_name).readall())
            except Exception as e:
                print(e)
                print("Something went wrong with the file transfer from {} to {}. Please check the logs and try again".format(self.__container_name, self.__local_folder_path))
        else:
            print("No blobs found containing the characters: {} or the size of the download exceeds the available disk on the compute target".format(self.__cloud_folder_path))
            
            
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
        local_file_list = [file_name for file_name in local_file_list if (file_name not in self.__target_blobs) and not ('.amlignore' in file_name)]  #I hate this line of code but it's otherwise really inefficient      
        #upload the files that are left using the container client
        
        
          
        for upload_file in local_file_list:
            
            with open(file = os.path.join(self.__local_folder_path, upload_file), mode = 'rb') as data:
                print(os.path.join(destination_folder, upload_file))
                blob_client = container_client.upload_blob(name = os.path.join(destination_folder, upload_file), data = data, overwrite = False)
        print("Upload Complete, please verify with Azure Storage Explorer")
    #endregion


