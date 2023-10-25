import os
import zipfile
import io
import base64
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from smart_open import open


logging.getLogger("smart_open").setLevel(logging.CRITICAL)


class BlobService:
    def __init__(self, blob_provider="s3") -> None:
        self.blob_provider = os.environ.get("BLOB_PROVIDER", blob_provider)
        self.path = None

    def get_s3_client(self):
        import boto3

        logging.getLogger("boto3").setLevel(logging.CRITICAL)
        logging.getLogger("botocore").setLevel(logging.CRITICAL)
        logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
        logging.getLogger("urllib3").setLevel(logging.CRITICAL)

        session = boto3.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        return session.client("s3")

    def get_gcp_client(self):
        from google.cloud.storage import Client

        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None)
        if service_account_path is not None:
            client = Client.from_service_account_json(service_account_path)
        else:
            from google.auth.credentials import Credentials

            token = os.environ["GOOGLE_API_TOKEN"]
            credentials = Credentials(token=token)
            client = Client(credentials=credentials)
        return client

    def get_azure_client(self):
        from azure.storage.blob import BlobServiceClient

        azure_storage_connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        return BlobServiceClient.from_connection_string(azure_storage_connection_string)

    def get_blob_client(self):
        if self.blob_provider == "s3":
            return self.get_s3_client()
        elif self.blob_provider == "gcp":
            return self.get_gcp_client()
        elif self.blob_provider == "azure":
            return self.get_azure_client()
        else:
            raise NotImplementedError(
                "Only supports the following providers at the moment: (s3, gcp, azure)"
            )

    def get_blob_path(self):
        if self.blob_provider == "s3":
            return f"s3://{os.environ['AWS_BUCKET_NAME']}/"
        elif self.blob_provider == "gcp":
            return f"gs://{os.environ['GOOGLE_BUCKET_NAME']}/"
        elif self.blob_provider == "azure":
            return f"azure://{os.environ['AZURE_BUCKET_NAME']}/"
        else:
            raise NotImplementedError(
                "Only supports the following providers at the moment: (s3, gcp, azure)"
            )

    @staticmethod
    def create_zipfile_buffer_from_dir(
        basedir,
        exclusions=None,
    ):
        zip_bytes_io = io.BytesIO()
        base_dir = basedir.rstrip("/")
        base_name = os.path.dirname(base_dir)
        exclusions = exclusions or []
        with zipfile.ZipFile(zip_bytes_io, "w", zipfile.ZIP_DEFLATED) as zipped:
            for dirpath, subdirs, files in os.walk(basedir):
                keyname = dirpath.replace(base_dir, "").strip("/")
                skip = False
                for excl in exclusions:
                    if dirpath.startswith(os.path.join(base_dir, excl)):
                        skip = True
                        break
                if skip:
                    continue
                zipped.write(dirpath, keyname)
                for filename in files:
                    print(filename)
                    zipped.write(
                        os.path.join(dirpath, filename), os.path.join(keyname, filename)
                    )
        return zip_bytes_io.getbuffer()

    @staticmethod
    def get_unique_key(base_key=""):
        unique_key = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + base64.b64encode(os.urandom(32))[:8].decode()
        if base_key:
            unique_key = base_key + "_" + unique_key
        return unique_key.replace("/", "")

    def upload_file(self, key, content=None, filepath=None):
        if self.path is None:
            self.path = self.get_blob_path()

        url = f"{self.path.strip('/')}/{key.strip('/')}"
        client = self.get_blob_client()

        if filepath is not None:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"{filepath} doesn't exist")
            with open(filepath, "rb") as fin:
                content = fin.read()
        elif content is None:
            raise ValueError("A byte format content or a valid filepath is required")

        with open(url, "wb", transport_params={"client": client}) as fout:
            bytes_written = 0
            bytes_written += fout.write(content)
            print(f"Published {bytes_written} bytes to remote!!!")

        del client
        return url

    def download_file(self, blob_url, save_dir, extract_files=True):
        client = self.get_blob_client()

        savepath = os.path.join(save_dir, blob_url.split("/")[-1])
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        with open(blob_url, "rb", transport_params={"client": client}) as fin:
            with open(savepath, "wb") as fout:
                for buffer in fin:
                    fout.write(buffer)

        del client

        if savepath.endswith(".zip") and extract_files:
            filepath = savepath
            savepath = os.path.join(save_dir, os.path.splitext(os.path.split(savepath)[-1])[0])
            archive_file = zipfile.ZipFile(filepath)
            for name in archive_file.namelist():
                if name.endswith("/"):
                    Path(os.path.join(savepath, name)).mkdir(exist_ok=True, parents=True)
                else:
                    with archive_file.open(name) as file:
                        content = file.read()
                    with open(os.path.join(savepath, name), "wb") as file:
                        file.write(content)
            os.remove(filepath)
        return savepath

    def bulk_upload(self, basedir, base_key="", exclusions=None, zip_data=False):
        base_key = base_key.strip("/")

        if zip_data:
            base_key = base_key or self.get_unique_key() + ".zip"
            if not base_key.endswith(".zip"):
                base_key += ".zip" 
            return self.upload_file(
                base_key,
                content=self.create_zipfile_buffer_from_dir(
                    basedir=basedir, exclusions=exclusions
                ),
            )
        
        s3_uri = None
        exclusions = exclusions or []
        for dirpath, dirname, filepaths in os.walk(basedir):
            keyname = dirpath.replace(basedir, "").strip("/")
            skip = False
            for excl in exclusions:
                if dirpath.startswith(os.path.join(basedir, excl)):
                    skip = True
                    break
            if skip:
                continue
            for filepath in tqdm(filepaths):
                file_key = f"{base_key}/{keyname}" if base_key else keyname
                file_key = os.path.join(file_key, filepath)
                _s3_uri = self.upload_file(
                    file_key,
                    filepath=os.path.join(dirpath, filepath),
                )
                if s3_uri is None:
                    s3_uri = _s3_uri.replace(filepath, "")
        return s3_uri
