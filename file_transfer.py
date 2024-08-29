'''
author: Ben Wade
email: ben.wade@noaa.gov
If I pooched it, let me know
'''

import os
import psutil
import hashlib
from sys import exit
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.ai.ml import MLClient
from azureml.fsspec.spec import AzureMachineLearningFileSystem
from datetime import datetime
import traceback




class FileTransferClient:
    #region init
    def __init__(self,
                 input_uri:str = None,
                 local_folder:str = None
                 ):
        """For Blobstore only: Blob URI: https://<your storage account>.<'blob'>.core.windows.net \n
           For Fileshare only: File URI: copy the "Datastore URI" listed in the data asset page on ML Studio. begins with 'azureml://'
           For Blobs Only, leave blank if using a fileshare: Container Name: Name of the container who's parent directory is the storage account in azure storage explorer\n
           Local Folder: Which folder you want to put these files in. Will be the root folder and cloud structure will be preserved\n
           Cloud Folder: Where the name of the folder you want to copy down from the cloud. All child folders will be copied with it\n"""
        #establish the local container variables
        self.__input_uri = input_uri
        self.__blob_uri = None
        self.__file_uri = None
        self.__container_name = None
        self.__local_folder_path = local_folder
        self.__blob_service_client = None
        self.__container_client = None
        self.__dstore_type = None
        self.__cloud_folder_path = None

        self.__uri_parser(self.__input_uri)
        if not hasattr(self, '__file_checksums'):
            self.__file_checksums = {}
        #make the destination folder
        if not os.path.exists(self.__local_folder_path):
            print("Target directory does not yet exist. Making target directory")
            os.makedirs(self.__local_folder_path)
        
        
        self.__to_gb = 1024.0**3
        
        #try to create login credentials
        try:
            self.__creds = DefaultAzureCredential()
        except Exception as e:
            print("The system encountered the following error: {} \n Error establishing credentials. Exiting.".format(e))
            exit(1)
        
        #try to create an as-of-yet unused ML Client
        try:
            pass
            #self.__ml_client = MLClient.from_config(credential=self.__creds)
        except Exception as e:
            
            print("The system encountered the following error: {} \n Error establishing ML Client Obejct. Exiting.".format(e))
            exit(1)
        
                    
        #if the storage container is a fileshare, we instantiate the stuff here
        if (self.__file_uri!=None) and (self.__cloud_folder_path!=None):
            try:
                self.__azmlfs = AzureMachineLearningFileSystem(uri = self.__file_uri)
            except Exception as e:
                print("The system encountered the following error: {} \n Error establishing Azure ML File System. Exiting.".format(e))
                exit(1)
            self.__target_files = []
            self.__get_target_file_list(key = self.__cloud_folder_path)
            


        else:
            print("blob/file logic failed")
            self.__how_did_you_get_here()

        
        self.__container_size = self.__get_container_size()
       
        self.__get_available_disk()
        
        self.__get_available_memory()
       
        self.__print_instructions()

        #endregion 
    def __how_did_you_get_here(self):
        print("You shouldn't be able to get to this print statement. Try restarting the kernal and reinstantiating this object")
        traceback.print_stack()
        exit(1)

    def __print_instructions(self):
        print("### This file transfer method uses Azure CLI Credentials. Please type 'az login' into the terminal to authenticate those credentials ### \n")
        print("\n This method will allow you to download a whole folder in a blob or file container and upload completed products to a folder in the same container. \n")
        print("\n This object assumes that any file you have processed before uploading has had its name changed or exists in a different directory than its raw source data \n")
        print("\n If you need to upload or download to a different container, you'll need to make another client and handle those methods there by exchanging file paths.\n")
        print("\n All SDK Items needed to transfer the files have been established. to download the files, please use the 'get_cloud_folder()' method \n To upload files, please use the 'put_local_folder()' method")
        
        
    def __uri_parser(self, input_uri:str = None):
        
        if("https://" in input_uri):
            print("Look like you pulled the 'DataStore URI' instead of the 'Storage URI'")
            print("Azure has a lot of security related eccentricities. When pulling data from azure")
            print("please take care to use the 'Datastore URI' that begins with 'azureml://subscriptions/<etc>'.")
            print("You can find that URI under the 'Data->datastores-><your fileshare>->browse page in")
            print("azure machine learning studio")
            print("PATH = "+ input_uri)
            exit(1)
        
        split_path = input_uri.partition('paths')
        self.__file_uri = split_path[0][:-1]
        self.__cloud_folder_path = split_path[2][1:]

        
    
    #region testing functions            
    #helper function to get blob names and folder structure
    '''
    def print_blob_names(self):
        
        if(self.__target_blobs):
            for x in range(0, len(self.__target_blobs)):
                print(self.__target_blobs[x])
                
    def print_file_names(self):
        if(self.__target_files):
            for x in range(0, len(self.__target_files)):
                print(self.__target_files[x])

    def __try_copy(self):
        self.__copy_folder_structure()
    #endregion
    '''
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

        #if its a file, use the fsspec sizes tool to get the sizes from the globs
        if(self.__azmlfs):
            size = self.__azmlfs.sizes(paths = self.__target_files)
            size = sum(size)
        #if its something else, something has gone horribly wrong
        else:
            self.__how_did_you_get_here()
        
        size = size/self.__to_gb
        print('download size in gb is {}'.format(size))
        return size
    
    #endregion
    
    #region file handling functions
    
    def __get_target_file_list(self, key:str = None):
        
        if self.__azmlfs:
           # try:
            buffer = self.__azmlfs.glob(key+'**')
            for name in buffer:
                if(key in name) and not ('.aml' in name) and not(name[-1]=='/'):
                    self.__target_files.append(name)
                else:
                    
                    pass

                
            #except Exception as e:
            #    print(e)
            #    print('Sorry, no files found with that folder path in the chosen data asset')
        else:
            self.__how_did_you_get_here()
    
    #in case the file already exists, this appends a timestamp to the filename so it won't overwrite anything
    def __change_upload_file_name(self, upload_file:str)->str:
        print("WARNING: This tool cannot overwrite any existing data in the cloud.")
        print("The filename has been updated with the current time to avoid data loss in the cloud")
        now = datetime.now().strftime('-edited-%Y-%m-%d-%H-%M-%S')
        #There has to be a more condensed way to do this, but I'm kinda at the end of my rope here and I don't want to think too hard.
        split_path = upload_file.split('.')
        extension = split_path[-1]
        filename = split_path[0]+now
        local_filename = os.path.join(self.__local_folder_path, *[self.__cloud_folder_path, upload_file])
        upload_file = filename+'.'+extension
        new_filename = os.path.join(self.__local_folder_path, *[self.__cloud_folder_path, upload_file])
        os.rename(os.path.join(local_filename), os.path.join(new_filename))
        
        return upload_file
    #endregion
    
    #region transfer functions

 
    def get_cloud_folder(self):
        if self.__azmlfs:
            self.__transfer_from_file_to_compute()
        else:
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
            print("No source folder found, Using folder from Object instantiation")
            source_folder = self.__local_folder_path+'/'+self.__cloud_folder_path
        
        if self.__azmlfs:
            self.__upload_folder_to_file(source_folder = source_folder, destination_folder = destination_folder)
        else:
            self.__how_did_you_get_here()
            

    #endregion

    #region files
    
    
    def __compute_checksum_sha256(self, file_path):
        here = os.getcwd()
        file_path = os.path.join(here, file_path)
        print("Checking file: {}".format(file_path))

        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def __transfer_from_file_to_compute(self):
        self.__get_available_disk()
        self.__get_available_memory()

        if(len(self.__target_files)!=0) and (self.__free_disk>self.__container_size):
            try:
                self.__azmlfs.start_transaction()
                for path in self.__target_files:
                    try:
                        
                        self.__azmlfs.get_file(rpath = path, lpath= self.__local_folder_path)
                        here = os.getcwd()
                        local_path = os.path.join(here, self.__local_folder_path[2:], path)
                        
                        checksum = self.__compute_checksum_sha256(local_path)
                        print("Checksum: {}".format(checksum))
                        self.__file_checksums[local_path] = checksum
                        print(self.__file_checksums.get(local_path))

                    except Exception as e:
                        print(e)
                self.__azmlfs.end_transaction()
            except Exception as e:
                self.__azmlfs.end_transaction()
                print("Something went wrong with the file transfer from {} to {}. Please check the logs and try again".format(self.__container_name, self.__local_folder_path))
        else:
            print("No files found containing the characters: {} or the size of the download exceeds the available disk on the compute target".format(self.__cloud_folder_path))
        
        
    
    def __upload_folder_to_file(self, source_folder:str = None, destination_folder:str = None):
        local_file_list = os.listdir(source_folder)
        #strip out the files that were downloaded to begin with
        local_file_list = [file_name for file_name in local_file_list if (file_name not in self.__target_files) and not ('.amlignore' in file_name)]  #I hate this line of code but it's otherwise really inefficient      
        #upload the files that are left using the container client
       
        try:
            fail_list = []
            self.__azmlfs.start_transaction()
            for upload_file in local_file_list:
                print("Uploading file: {}".format(upload_file))
                
                local_file_path = os.path.join(source_folder[2:], upload_file)
                print(local_file_path)
                checksum = self.__compute_checksum_sha256(local_file_path)
                local_file_path = os.path.join(os.getcwd(), local_file_path)
                # Check if the file exists in self.__file_checksums and if the checksum has changed
                if upload_file not in self.__file_checksums or self.__file_checksums[upload_file] != checksum:
                    cloud_file_path = os.path.join(destination_folder, upload_file)
                    is_existing_file = self.__azmlfs.isfile(cloud_file_path)
                    if not is_existing_file or (is_existing_file and self.__file_checksums.get(local_file_path, '') != checksum):
                        # Proceed with upload
                        try:
                            # Assuming there's a method like put_file for uploading
                            self.__azmlfs.put_file(rpath=os.path.join(destination_folder, upload_file), lpath=local_file_path)
                            print("Uploaded file: {}".format(upload_file))
                            #update the checksum in case it changes again in teh same session
                            self.__file_checksums[local_file_path] = self__compute_checksum_sha256(local_file_path)
                        except Exception as upload_error:
                            print(f"Failed to upload {upload_file}: {upload_error}")
                            fail_list.append(upload_file)
                        else:
                            print("{} not uploaded because it already exists in the cloud and hasn't been changed".format(upload_file))
            self.__azmlfs.end_transaction()
            if(len(fail_list)!=0):
                print("Encountered an error during upload. The following files could not be uploaded. Please try again")
                for file in fail_list:
                    print(file)
            else:
                print("Upload Complete, please verify with Azure Storage Explorer")
            
        except Exception as e:
            print(e)
            print("an exception occurred, probably because another user is uploading to the same location. Please try again")
            self.__azmlfs.end_transaction()
    #endregion
    #endregion
