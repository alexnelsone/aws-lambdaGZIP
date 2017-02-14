import boto3
import os
import zipfile
import gzip
import logging
import time
import re
import urllib
import shutil

# list all files in a bucket
# make a backup of the file
# make a gzip of the file

#set bucket name manually for testing
#bucketName = 'dev-med-bia-testbucket'
bucketName = 'put_your_bucket_name_here'

logging.basicConfig()
logger = logging.getLogger()
# for local testing set profile
#boto3.setup_default_session(profile_name='nelsone')
current_session = boto3.session.Session()

current_region = current_session.region_name

s3client = boto3.client('s3')
#seems you can only make a backup of a file with a resource
s3resource = boto3.resource('s3')

#a regex to find the files I'm looking for
regex = re.compile(r'^ghx/hadoop-upload/.*txt.zip$')


def gzip_file(file):
    #this function takes in a file and gzips the file.
    print('running gzip on ' + str(file))
    try:
        with open(file, 'rb') as file_in, gzip.open('/tmp/'+ file.split(".")[0]+'.gz', 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)
            return True
    except Exception as e:
        logger.warn('gzip_file err: ' + str(e))
        return False
        
def upload_gz_file(file):
    # this function uploads the gzip file that was created to the S3 bucket.
    try:
        file_name = file.split(".")[0]+'.gz'
        file_to_upload = 'ghx/hadoop-upload-gz/' + file_name
        print('upload to: '+bucketName + file_to_upload)
        #s3resource.Bucket(bucketName).upload_file('/tmp/'+file_name,file_to_upload)
        s3resource.meta.client.upload_file('/tmp/'+file_name, bucketName, file_to_upload)
        return True
    except Exception as e:
        print(str(e))
        return False


def backup_files_to_gzip(s3Objects):
    # this function lists the file in the bucket that we are going to work with
    # and makes a backup of the file by copying it and adding the extension '.original'
    for content in s3Objects["Contents"]:
        if re.match(regex, content['Key']):
            bucket_file = (content['Key'])
            fileName = bucket_file.split("/")[2]
            print("copying " + fileName)
            s3resource.Object(bucketName,bucket_file+'.original').copy_from(CopySource=bucketName+'/'+bucket_file)
            s3client.download_file(bucketName, bucket_file, '/tmp/'+fileName)
            zipFile = zipfile.ZipFile('/tmp/'+fileName)
            files = zipFile.namelist()
            for file in files:
                print(str(file))
                zipFile.extract(str(file),"/tmp/")
            gzip_file(file)
            upload_gz_file(file)
            #return True here just to run against one file for testing.
            return True


def lambda_handler(event, context):

    s3Objects = s3client.list_objects_v2(Bucket=bucketName)
    response = backup_files_to_gzip(s3Objects)
    
    
    
lambda_handler("","")