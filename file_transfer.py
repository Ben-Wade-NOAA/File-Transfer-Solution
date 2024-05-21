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
from azureml.fsspec.spec import AzureMachineLearningFileSystem
from datetime import datetime





class FileTransferClient:
    #region init
    def __init__(self,
                 blob_uri:str = None,
                 file_uri:str = None, 
                 container_name:str = None, 
                 local_folder:str = './',
                 cloud_folder:str = None
                 ):
        """For Blobstore only: Blob URI: https://<your storage account>.<'blob'>.core.windows.net \n
           For Fileshare only: File URI: copy the "Datastore URI" listed in the data asset page on ML Studio. begins with 'azureml://'
           For Blobs Only, leave blank if using a fileshare: Container Name: Name of the container who's parent directory is the storage account in azure storage explorer\n
           Local Folder: Which folder you want to put these files in. Will be the root folder and cloud structure will be preserved\n
           Cloud Folder: Where the name of the folder you want to copy down from the cloud. All child folders will be copied with it\n"""
        #establish the local container variables
        self.__blob_uri = blob_uri
        self.__container_name = container_name
        self.__local_folder_path = local_folder
        self.__blob_service_client = None
        self.__container_client = None
        
        if(blob_uri!=None and file_uri!= None):
            print("This tool is setup to only handle one blob or one fileshare at a time. Please remove one or the other and try again")
            exit(1)
        else:
            if blob_uri!=None:
                self.__dstore_type = 'blob'
            elif file_uri!=None:
                self.__dstore_type = 'file'
            else:
                print("there must be values provided to 'blob_uri' or 'file_uri'. Please enter a value for either variable and try again")
                exit(1)
        
        
        #make the destination folder
        if not os.path.exists(self.__local_folder_path):
            print("Target directory does not yet exist. Making target directory")
            os.mkdir(self.__local_folder_path)
        
        self.__cloud_folder_path = cloud_folder
        self.__to_gb = 1024.0**3
        
        #try to create login credentials
        try:
            self.__creds = DefaultAzureCredential()
        except Exception as e:
            print("The system encountered the following error: {} \n Error establishing credentials. Exiting.".format(e))
            exit(1)
        
        #try to create an as-of-yet unused ML Client
        try:
            self.__ml_client = MLClient.from_config(credential=self.__creds)
        except Exception as e:
            
            print("The system encountered the following error: {} \n Error establishing ML Client Obejct. Exiting.".format(e))
            exit(1)
        
        #if the storage container is a blobshare, we istantiate that stuff here
        if self.__dstore_type =='blob':
            #try to make a blob client
            try:
                self.__blob_service_client = BlobServiceClient(account_url = self.__blob_uri, credential=self.__creds)
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
            #get a list of all the blobs in that container
            self.__get_target_blob_list(key = self.__cloud_folder_path)
            
        #if the storage container is a fileshare, we instantiate the stuff here
        elif self.__dstore_type == 'file':
            try:
                self.__azmlfs = AzureMachineLearningFileSystem(uri = file_uri)
            except Exception as e:
                print("The system encountered the following error: {} \n Error establishing Azure ML File System. Exiting.".format(e))
                exit(1)
            self.__target_files = self.__get_target_file_list(key = self.__cloud_folder_path)
            print(self.__target_files)
        else:
            self.__how_did_you_get_here()

        self.__container_size = self.__get_container_size()
        self.__get_available_disk()
        self.__get_available_memory()
        self.__print_instructions()

        #endregion 
    def __how_did_you_get_here(self):
        print("You shouldn't be able to get to this print statement. Try restarting the kernal and reinstantiating this object")
        exit(1)

    def __print_instructions(self):
        print("### This file transfer method uses Azure CLI Credentials. Please type 'az login' into the terminal to authenticate those credentials ### \n")
        print("\n This method will allow you to download a whole folder in a blob container and upload completed products to a folder in the same container. \n")
        print("\n This object assumes that any file you have processed before uploading has had its name changed or exists in a different directory than its raw source data \n")
        print("\n If you need to upload or download to a different container, you'll need to make another client and handle those methods there by exchanging file paths.\n")
        print("\n If you find any logical errors or bugs, please email the author listed in the source code \n")
        
    def __blob_or_file(self)->bool:
        dstore_type = None
        if('.blob.' in self.__blob_uri):
            dstore_type = 'blob'
        elif('.file.' in self.__blob_uri):
            dstore_type = 'file'
        else:
            dstore_type = 'invalid'
        
        return dstore_type
    
    #region testing functions            
    #helper function to get blob names and folder structure
    def print_blob_names(self):
        print(self.__target_blobs)
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
        #helper
        print("Free Disk: "+str(self.__free_disk))
    #gets available ram from the OS on the local compute
    #here's a staged change
    
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
        print('Free Memory: '+str(self.__free_mem))
        
    #reads the size of the items to be downloaded from the cloud
    def __get_container_size(self)->float:
        size = 0.0
        #if its a blob, use the blob properties dictionary to get the size
        if(self.__dstore_type=='blob'):
            container_client = self.__container_client
            for blob in self.__target_blobs:
                blob_client = container_client.get_blob_client(blob)
                size+= blob_client.get_blob_properties().size
        #if its a file, use the fsspec sizes tool to get the sizes from the globs
        elif(self.__dstore_type=='file'):
            size = self.__azmlfs.sizes(paths = self.__target_files)
        #if its something else, something has gone horribly wrong
        else:
            self.__how_did_you_get_here()
        
        print('download size in bytes is {}'.format(size))
        return size/self.__to_gb
    
    #endregion
    
    #region file handling functions
    def __get_target_blob_list(self, key:str = None):
        if self.__container_client:
            container_client = self.__container_client
            print(container_client.list_blob_names())
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
    
    def __get_target_file_list(self, key:str = None):
        
        if self.__azmlfs:
            try:
                buffer = self.__azmlfs.glob(key+'*')
                for name in buffer:
                    if(key in name) and not ('.aml' in name):
                        self.__target_files.append(name)
                    else:
                        pass
            except Exception as e:
                print(e)
                print('Sorry, no files found with that folder path in the chosen data asset')
        else:
            self.__how_did_you_get_here()

    #takes the folder structure of the target blobs and copies it to the local compute cluster    
    def __copy_folder_structure(self):
        print('copy folder structure')
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
    
    def __change_upload_file_name(self, upload_file:str)->str:
        print("WARNING: This tool cannot overwrite any existing data in the cloud.")
        print("The filename has been updated with the current time to avoid data loss in the cloud")
        now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        upload_file = upload_file.split('.')
        extension = upload_file[-1]
        filename = upload_file[0]+now
        upload_file = filename+'.'+extension
        print("New File name: "+upload_file)
        return upload_file
    #endregion
    
    #region transfer functions
    def print_storage_type(self):
        print(self.__dstore_type)
    #these two functions call the correct upload and download functions
    #based on the storage account type
    def get_cloud_folder(self):
        if self.__dstore_type == 'blob':
            print('blob-transfer function')
            self.__transfer_from_blob_to_compute()
        elif self.__dstore_type == 'file':
            self.__transfer_from_file_to_compute()
        else:
            print('how did you get here get_cloud_folder')
            self.__how_did_you_get_here()
    
    def put_local_folder(self, source_folder:str = None, destination_folder:str = None):
        #these next few lines just make sure to handle using the original folders from the object
        #but you can send these files to whichever folder you want
        if(destination_folder == None):
            print("No destination folder found. Using folder from object instantiation")
            destination_folder = self.__cloud_folder_path
        else:
            destination_folder = destination_folder

        if(source_folder ==None):
            print("No source folder found, Using folder from Object inantiation")
            source_folder = self.__local_folder_path+'/'+self.__cloud_folder_path
        
        if self.__dstore_type == 'blob':
            self.__upload_folder_to_blob(source_folder= source_folder, destination_folder= destination_folder)
        elif self.__dstore_type=='file':
            self.__upload_folder_to_file(source_folder = source_folder, destination_folder = destination_folder)
        else:
            self.__how_did_you_get_here()
            
    #region blobs
    #TODO timeout failsafe
    #TODO all or nothing processes in try catch or cleanup partial downloads - or just retry the files that didn't make it
    #TODO maybe add a data cap so that huge downloaded don't happen. if you have bigger file sizes, move to batchs
    #TODO put a lock on fileshare/blobs when downloading
    def __transfer_from_blob_to_compute(self):
        """Will transfer a folder and all its contents from a blob into the folder you specified when you created the object.\n
        The folder structure will be preserved, but currently cannot copy files from a folders that do not share the same parent directory."""
        blob_service_client = self.__blob_service_client
        container_client = blob_service_client.get_container_client(self.__container_name)
        print('__transfer from blob to compute')       
        self.__get_available_disk()
        self.__get_available_memory()
        
        if(len(self.__target_blobs)!=0)and(self.__free_disk>self.__container_size):#check to see if there's enough room on the local compute
            try:
                #timeouts are limited to 2 minutes per mb max, so this sets the timeouts to max.
                timeout = 30.0#(self.__container_size/1000.0)*119
                
                self.__copy_folder_structure()#copies to the folder structure
                for blob_name in self.__target_blobs:#loops through each blob

                    blob_client = container_client.get_blob_client(blob = blob_name)
                    print(blob_client.exists())
                    cleaned_blob_name = blob_name.split('/')#probably doesn't need to happen, but splits up by folders

                    try:
                        lease = blob_client.acquire_lease()
                        #download each blob 1x1
                        target_path = os.path.join(self.__local_folder_path, *cleaned_blob_name)
                        with open(file = target_path, mode = 'wb') as download_file:
                            download_file.write(blob_client.download_blob().readall())
                        lease.break_lease()
                    except Exception as e:
                        lease.break_lease()
                #cleanup the lease and container client
                    container_client.close()
            except Exception as e:
                print("Something went wrong with the file transfer from {} to {}. Please check the logs and try again".format(self.__container_name, self.__local_folder_path))
        else:
            print("No blobs found containing the characters: {} or the size of the download exceeds the available disk on the compute target".format(self.__cloud_folder_path))
            
    #helper function handles setting the source and destination folders correctly        
    def __upload_folder_to_blob(self, source_folder:str = None, destination_folder:str = None):
        #get the right clients
        local_blob_client = self.__blob_service_client
        container_client = local_blob_client.get_container_client(container=self.__container_name)
        #get files in whatever directory you're trying to upload
        local_file_list = os.listdir(source_folder)
        print("before processing: ", local_file_list)
        #strip out the files that were downloaded to begin with
        local_file_list = [file_name for file_name in local_file_list if (file_name not in self.__target_blobs) and not ('.amlignore' in file_name)]  #I hate this line of code but it's otherwise really inefficient      
        #upload the files that are left using the container client
        
        print(local_file_list)
        print(source_folder)
        print(self.__local_folder_path)

        try:
            #put a lock on the containers where this stuff is going
        #    lease = container_client.lease(lease_timeout = -1)  
            for upload_file in local_file_list:
                
                blob_client = container_client.get_blob_client(blob = os.path.join(destination_folder, upload_file))
                is_old_blob = blob_client.exists()
                print("is old blob: "+str(is_old_blob))
                if is_old_blob:
                    print("A blob with the provided name exists. The name is being changed to prevent data loss")
                    updated_upload_file = self.__change_upload_file_name(upload_file=upload_file)
                else:
                    print("Creating blob for {}".format(upload_file))
                    updated_upload_file = upload_file
                
                with open(file = os.path.join(source_folder, upload_file), mode = 'rb') as data:
                    print(type(data))
                    print(os.path.join(destination_folder, upload_file))
                    blob_client = container_client.upload_blob(name = os.path.join(destination_folder, updated_upload_file), data = data, overwrite = False)
            print("Upload Complete, please verify with Azure Storage Explorer")

        except Exception as e:
            print(e)
            print("an exception occurred, probably because another user is uploading to the same location. Please try again")
        container_client.close()
      
   
    #endregion

    #region files
    #TODO timeout failsafe
    #TODO all or nothing processes in try catch or cleanup partial downloads - or just retry the files that didn't make it
    #TODO maybe add a data cap so that huge downloaded don't happen. if you have bigger file sizes, move to batchs
    #TODO put a lock on fileshare/blobs when downloading
    def __transfer_from_file_to_compute(self):
        self.__get_available_disk()
        self.__get_available_memory()

        if(len(self.__target_files)!=0) and (self.__free_disk>self.__container_size):
            try:
                self.__azmlfs.start_transaction()
                for path in self.__target_files:
                    try:
                        self.__azmlfs.get_file(rpath = path, lpath= self.__local_folder_path)
                    except Exception as e:
                        print(e)
                self.__azmlfs.end_transaction()

            except Exception as e:
                self.__azmlfs.end_transaction()
                print("Something went wrong with the file transfer from {} to {}. Please check the logs and try again".format(self.__container_name, self.__local_folder_path))
        else:
            print("No files found containing the characters: {} or the size of the download exceeds the available disk on the compute target".format(self.__cloud_folder_path))
        
        pass
    
    def __upload_folder_to_file(self, source_folder:str = None, destination_folder:str = None):
        
        
        local_file_list = os.listdir(source_folder)
        #strip out the files that were downloaded to begin with
        local_file_list = [file_name for file_name in local_file_list if (file_name not in self.__target_blobs) and not ('.amlignore' in file_name)]  #I hate this line of code but it's otherwise really inefficient      
        #upload the files that are left using the container client
        
        print(local_file_list)
        print(source_folder)
        print(self.__local_folder_path)
       
        try:
            fail_list = []
            self.__azmlfs.start_transaction()
            for upload_file in local_file_list:
                try:
                    self.__azmlfs.put_file(lpath = upload_file, rpath = destination_folder)
                except Exception as e:
                    print(e)
                    fail_list.append(upload_file)
            self.__azmlfs.end_transaction()
            if(len(fail_list!=0)):
                print("Error, the following files could not be uploaded. Please try again")
                for file in fail_list:
                    print(file)
            print("Upload Complete, please verify with Azure Storage Explorer")
            
        except Exception as e:
            print("an exception occurred, probably because another user is uploading to the same location. Please try again")
            self.__azmlfs.end_transaction()
    #endregion
    #endregion
