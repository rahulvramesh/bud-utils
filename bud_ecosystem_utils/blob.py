import os
import logging
from tqdm import tqdm
from smart_open import open


logging.getLogger('smart_open').setLevel(logging.CRITICAL)


class BlobService:
    def __init__(self, blob_provider="s3") -> None:
        self.blob_provider = os.environ.get('BLOB_PROVIDER', blob_provider)
        self.path = None

    def get_s3_client(self):
        import boto3
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
        logging.getLogger('urllib3').setLevel(logging.CRITICAL)
        
        session = boto3.Session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        )
        return session.client('s3')

    def get_gcp_client(self):
        from google.cloud.storage import Client
        service_account_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', None)
        if service_account_path is not None:
            client = Client.from_service_account_json(service_account_path)
        else:
            from google.auth.credentials import Credentials
            token = os.environ['GOOGLE_API_TOKEN']
            credentials = Credentials(token=token)
            client = Client(credentials=credentials)
        return client

    def get_azure_client(self):
        from azure.storage.blob import BlobServiceClient
        azure_storage_connection_string = os.environ['AZURE_STORAGE_CONNECTION_STRING']
        return BlobServiceClient.from_connection_string(azure_storage_connection_string)

    def get_blob_client(self):
        if self.blob_provider == "s3":
            return self.get_s3_client()
        elif self.blob_provider == "gcp":
            return self.get_gcp_client()
        elif self.blob_provider == "azure":
            return self.get_azure_client()
        else:
            raise NotImplementedError("Only supports the following providers at the moment: (s3, gcp, azure)")
    
    def get_blob_path(self):
        if self.blob_provider == "s3":
            return f"s3://{os.environ['AWS_BUCKET_NAME']}/"
        elif self.blob_provider == "gcp":
            return f"gs://{os.environ['GOOGLE_BUCKET_NAME']}/"
        elif self.blob_provider == "azure":
            return f"azure://{os.environ['AZURE_BUCKET_NAME']}/"
        else:
            raise NotImplementedError("Only supports the following providers at the moment: (s3, gcp, azure)")

    def upload_file(self, key, filepath):
        if self.path is None:
            self.path = self.get_blob_path()

        url = f"{self.path.strip('/')}/{key.strip('/')}"
        client = self.get_blob_client()

        with open(filepath, 'rb') as fin:
            content = fin.read()

        with open(url, 'wb', transport_params={'client': client}) as fout:
            bytes_written = 0
            bytes_written += fout.write(content)
            print(f"Published {bytes_written} bytes to remote!!!")

        del client

    def bulk_upload(
        self, basedir, base_key="", exclusions=None, public=False
    ):
        exclusions = exclusions or []
        for dirpath, dirname, filepaths in os.walk(basedir):
            keyname = dirpath.replace(basedir, "").strip("/")
            skip = False
            for excl in exclusions:
                if dirpath.startswith(os.join(basedir, excl)):
                    skip = True
                    break
            if skip:
                continue
            for filepath in tqdm(filepaths):
                self.upload_file(
                    os.join(dirpath, filepath),
                    f"{base_key}/{keyname}" if base_key else keyname,
                )