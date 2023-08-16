import os
import struct
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential

sas = '?sv=2022-11-02&ss=b&srt=sco&sp=rwacx&se=2024-08-12T11:45:24Z&st=2023-08-13T03:45:24Z&spr=https&sig=WXkeyRx8i4%2F%2F%2BywCszT%2FBTWlWZZqZfpB9yR7m2QvkxE%3D'

class AzureDataLake:
    def __init__(self, account_name, sas_token):
        self.service_client = self.get_service_client_sas(account_name, sas_token)
    
    def get_service_client_sas(self, account_name: str, sas_token: str) -> DataLakeServiceClient:
        account_url = f"https://{account_name}.dfs.core.windows.net"

        # The SAS token string can be passed in as credential param or appended to the account URL
        service_client = DataLakeServiceClient(account_url, credential=sas_token)

        return service_client

    def create_file_system(self, file_system_name: str) -> FileSystemClient:
        file_systems = self.service_client.list_file_systems()
        
        for file_system in file_systems:
            if file_system_name == file_system.name:
                return self.service_client.get_file_system_client(file_system_name)
                        
        file_system_client = self.service_client.create_file_system(file_system=file_system_name)
        return file_system_client

    def create_directory(self, file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
        directory_client = file_system_client.create_directory(directory_name)

        return directory_client

    def upload_json_file_to_directory(self, directory_client: DataLakeDirectoryClient, file_name: str, file_contents: list):
        # file_client = directory_client.get_file_client(file_name)
        try:
            file_client = directory_client.create_file(file_name)
            content = ''

            for row in file_contents:
                content = content + str(row) + '\n'

            content = content.replace("'", '"')
            file_client.append_data(data=content, offset=0, length=len(content))

            file_client.flush_data(len(content))
        except Exception as e:
            print(e)

    def append_data_to_file(self, directory_client: DataLakeDirectoryClient, file_name: str):
        file_client = directory_client.get_file_client(file_name)
        file_size = file_client.get_file_properties().size

        data = b"Data to append to end of file"
        file_client.append_data(data, offset=file_size, length=len(data))

        file_client.flush_data(file_size + len(data))