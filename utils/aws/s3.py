import logging
import os
import pathlib
import gzip
from urllib.parse import urlparse
import boto3
import json
from datetime import datetime
from botocore.client import Config
from botocore.exceptions import ClientError
from io import BytesIO, TextIOWrapper


class S3Handler:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
        max_pool_connections = kwargs.get('max_pool_connections', 50)
        if aws_access_key_id and aws_secret_access_key:
            self.s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                                              aws_secret_access_key=aws_secret_access_key,
                                              config=Config(signature_version='s3v4',
                                                            max_pool_connections=max_pool_connections))
        else:
            self.s3_resource = boto3.resource('s3', config=Config(signature_version='s3v4',
                                                                  max_pool_connections=max_pool_connections))
        self.s3_client = self.s3_resource.meta.client
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_session = None
        self.bucket_name = kwargs.get('bucket_name')
        logging.info(f'Max Pool connection variable: {self.s3_client.meta.config.max_pool_connections}')

    def get_client(self):
        return self.s3_client

    def put_object(self, bucket_name, key, body):
        self.s3_client.put_object(
            Body=body,
            Bucket=bucket_name,
            Key=key
        )

    def bucket_exists(self, bucket_name):
        """Determine whether bucket_name exists and the user has permission to access it

        :param bucket_name: string
        :return: True if the referenced bucket_name exists, otherwise False
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            return False
        return True

    def connection_pooling(self, bucket_name):
        logging.info(f'Checking S3 connection to {bucket_name}')
        self.s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f'S3 connect to {bucket_name} successfully')

    def get_s3_session(self):
        if self.s3_session:
            return self.s3_session
        return self.create_s3_session()

    def create_s3_session(self):
        if not self.aws_access_key_id:
            self.s3_session = boto3.session.Session()
        else:
            self.s3_session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        return self.s3_session

    def upload(self, filename, bucket, s3_path=None, s3_full_path=None, callback=None, extra_args=None):
        """
            Example:
                filename (local_file_path) = '/Users/eq-0100/Downloads/brand.xlsx'
                bucket_name = 'opdatalake-dev'
                s3_destination_path = "pricing_jobs/kam_input_backup/"
                s3_destination_full_path = "pricing_jobs/kam_input_backup/test.xlsx"
        """
        if not s3_full_path:
            if not s3_path:
                raise ValueError('We must have s3_full_path or s3_path')
            s3_full_path = os.path.join(s3_path, os.path.basename(filename))
        s3_full_path = str(pathlib.Path(s3_full_path))
        self.s3_client.upload_file(filename, bucket, s3_full_path,
                                   callback, extra_args)
        s3_url = 's3://{bucket}/{key}'.format(bucket=bucket, key=s3_full_path)
        return s3_url

    def get_list_item_from_path(self, bucket_name, s3_search_path):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=bucket_name,
            Prefix=s3_search_path
        )

        item_list_object = []
        for page in pages:
            if page.get('Contents'):
                for obj in page['Contents']:
                    item_list_object.append(obj)
        return item_list_object

    def search_files_in_s3_folder_by_extension(self, path, extension, is_return_file_detail=False):
        bucket_name = self.bucket_name
        bucket = self.s3_resource.Bucket(bucket_name)
        file_list = []
        for file in bucket.objects.filter(Prefix=path):
            if file.key.endswith(extension):
                file_url = f's3://{self.bucket_name}/{file.key}'
                if is_return_file_detail:
                    file_detail = {"file_url": file_url, "file_last_modified": file.last_modified}
                    file_list.append(file_detail)
                else:
                    file_list.append(file_url)
        return file_list

    def df_to_s3_gz_file(self, df, file_path):
        gz_buffer = BytesIO()
        with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
            df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)
        s3_resource = self.s3_resource
        s3_object = s3_resource.Object(self.bucket_name, file_path)
        s3_object.put(Body=gz_buffer.getvalue())

    def df_to_s3_csv_file(self, source_path, s3_path):
        """
            source_path: path to csv file in local
            s3_path: path to csv S3
            Note: This function doesn't include creating csv file in local and delete it when upload successfully
        """
        s3_resource = self.s3_resource
        s3_resource.meta.client.upload_file(Filename=source_path, Bucket=self.bucket_name, Key=s3_path)

    def read_file_from_s3(self, bucket_name, key):
        session = self.get_s3_session()
        s3 = session.client('s3')
        s3_object = s3.get_object(Bucket=bucket_name, Key=key)
        body = s3_object['Body']
        return body.read()

    @staticmethod
    def _get_bucket_name_and_file_path_from_s3_file_url(s3_url):
        parse_url_result = urlparse(s3_url)
        bucket_name = parse_url_result.netloc
        file_path = parse_url_result.path[1:]
        file_path = file_path[1:] if file_path.startswith('/') else file_path
        return bucket_name, file_path

    def download_files(self, prefix, out_file_path):
        objs = self.get_list_item_from_path(self.bucket_name, prefix)
        for obj in objs:
            obj = obj.get('Key')
            s3_file_url = f's3://{self.bucket_name}/{obj}'
            bucket_name, s3_file_path = self._get_bucket_name_and_file_path_from_s3_file_url(s3_file_url)
            file_name = s3_file_path.split('/')[-1]
            output_file = out_file_path + file_name
            logging.info(f'Download file to path: {output_file}')
            self.s3_client.download_file(bucket_name, s3_file_path, output_file)

    def write_dict(self, bucket_name: str, path: str, data: dict) -> None:
        """
        Write dictionary data to s3
        Parameters
        ----------
        bucket_name: str
            s3 bucket name
        path: str
            s3 path prefix
        data: dict
            The dictionary
        """
        self.put_object(
            bucket_name=bucket_name, key=path, body=json.dumps(data)
        )
        logging.info(f'Successfully write data to bucket: {bucket_name} path: {path}')

    def write_dict_with_time_partition(self, bucket_name: str, prefix: str, file_name: str, data: dict, time: datetime,
                                       partition: str) -> None:
        """
        Write dictionary data to s3
        Parameters
        ----------
        bucket_name: str
            s3 bucket name
        prefix: str
            s3 path prefix
        file_name: str
            s3 file name
        data: dict
            The dictionary
        time: datetime
            the time to write data
        partition: str
            the time partition: hourly or daily
        """
        path = ''
        if partition == 'hourly':
            path = f'{prefix}/{time.year}/{time.month:02d}/{time.day:02d}/{time.hour:02d}/{file_name}'
        elif partition == 'daily':
            path = f'{prefix}/{time.year}/{time.month:02d}/{time.day:02d}/{file_name}'
        else:
            raise ValueError("Wrong partition input value, should be daily/hourly ")
        self.write_dict(bucket_name=bucket_name, path=path, data=data)