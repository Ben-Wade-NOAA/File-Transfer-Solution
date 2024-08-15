download_files<-function(azure_uri, local_path){
    libary(reticulate)
    source_python('./file_transfer.py')
    client<-FileTransferClient(input_uri = azure_uri, local_folder = local_path)
    client$get_cloud_folder()

}

upload_files<-function(azure_uri, local_path){
    libary(reticulate)
    source_python('./file_transfer.py')
    client<-FileTransferClient(input_uri = azure_uri, local_folder = local_path)
    client$put_local_folder()
}