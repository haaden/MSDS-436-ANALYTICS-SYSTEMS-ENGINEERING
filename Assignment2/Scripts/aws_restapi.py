#***********************************************
# File: awsapi.py
# Desc: Python script to connect to AWS S3
# Purpose: Perform following operation:
#          1. List buckets
#          2. Create bucket
#          3. Delete bucket
#          4. Bucket exists
#          5. List Objects
#          6. Create folder
#          7. Delete folder
#          8. Folder exists
#          9. Upload file
#          10. Download file
#          11. Delete File
#          12. File exists
# Auth: Shreenidhi Bharadwaj
# Date: 9/29/2019
# ALL RIGHTS RESERVED | DO NOT DISTRIBUTE
#************************************************/

import os
import boto3
from botocore.exceptions import ClientError
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter
from flask import Flask, request, jsonify
from flask_restful import Resource, Api

app = Flask(__name__)


@app.route('/')
def api_root():
    return '<h1>AWS S3 REST API</h1>'

@app.route('/buckets')
def api_bucket():
    return '<h2>Choose valid operation on Bucket (/list, /create, /delete, /exists)</h2>'

@app.route('/folders')
def api_folder():
    return '<h2>Choose valid operation on Folder (/list, /create, /delete, /exists)</h2>'

@app.route('/files')
def api_file():
    return '<h2>Choose valid operation on File (/upload, /download, /delete, /exists)</h2>'


@app.route('/buckets/list')
def list_buckets():
    """Connect to S3 and query all buckets

    :return: list of buckets
    """

    s3 = boto3.client('s3')

    # Call S3 to list current buckets
    resp = s3.list_buckets()

    # Get a list of all bucket names from the response
    buckets = {"buckets" : [bucket['Name'] for bucket in resp['Buckets']]}

    response = jsonify(buckets)
    response.status_code = 200

    return(response)


@app.route('/buckets/exists/<bucket_name>')
def bucket_exists(bucket_name):
    """Determine whether bucket exists and the user has permission
    to access it

    :param bucket_name: AWS S3 bucket to be searched
    :return: True if the referenced bucket_name exists, otherwise False
    """

    s3 = boto3.client('s3')
    try:
        response = s3.head_bucket(Bucket=bucket_name)
        result = {"bucket_name": bucket_name, "exists" : True}

    except ClientError as e:
        result = {"bucket_name": bucket_name, "exists" : False}

    response = jsonify(result)
    response.status_code = 200

    return(response)


@app.route('/buckets/create')
def create_bucket():
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the users S3
    default region read from credentials in ~/.aws/config.

    :param bucket_name: Bucket to create
    :param region: region to create bucket in, e.g., 'us-west-2'
    :return: bucket created status
    """
    bucket_name = request.args.get("bucket_name")
    region = request.args.get("region")

    s3 = boto3.client('s3')
    try:
        flg_exists = s3.head_bucket(Bucket=bucket_name)
    except:
        flg_exists = None

    if not(flg_exists):
        try:
            if region is None:
                session = boto3.Session(profile_name='default')
                region = session.region_name
                location = {'LocationConstraint': region}
                s3 = session.client('s3')
                s3.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)

            else:
                s3 = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3.create_bucket(Bucket=bucket_name,
                                 CreateBucketConfiguration=location)

            response = jsonify({"bucket_name": bucket_name, "region" : region, "status" : 'bucket creation successful'})
            response.status_code = 201

        except ClientError as e:
            response = jsonify({"bucket_name": bucket_name, "region" : region, "status" : 'bucket creation failed'})
            response.status_code = 400
    else:
        response = jsonify({"bucket_name": bucket_name, "region" : region, "status" : 'bucket already exists'})
        response.status_code = 409

    return(response)


@app.route('/buckets/delete')
def delete_bucket():
    """Delete an empty S3 bucket

    If the bucket is not empty, the operation fails.

    :param bucket_name: AWS S3 bucket
    :return: bucket deletion status
    """
    bucket_name = request.args.get("bucket_name")


    s3 = boto3.client('s3')
    try:
        flg_exists = s3.head_bucket(Bucket=bucket_name)
    except:
        flg_exists = None

    if flg_exists:
        try:
            s3.delete_bucket(Bucket=bucket_name)

            response = jsonify({"bucket_name": bucket_name, "status" : 'bucket deletion successful'})
            response.status_code = 200

        except ClientError as e:
            response = jsonify({"bucket_name": bucket_name, "status" : 'bucket deletion failed, it may not be empty'})
            response.status_code = 403
    else:
        response = jsonify({"bucket_name": bucket_name, "status" : 'bucket not found'})
        response.status_code = 404

    return(response)


@app.route('/folders/list')
def list_objects():
    """List all directories & files starting with given object_name from an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param object_name: AWS S3 directory. If not specified then empty string
    :return: List of all the directories & files under given object_name(directory)
    """
    bucket_name = request.args.get("bucket_name")
    object_name = request.args.get("object_name")

    objects = []

    s3 = boto3.client('s3')
    try:
        if object_name is None:
            object_name = ''

        results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)

        try:
            for i in results['Contents']:
                objects.append(i['Key'])
        except:
            pass

        response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "content" : objects})
        response.status_code = 200

    except ClientError as e:
        response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "content" : 'Error'})
        response.status_code = 400

    return response


@app.route('/folders/exists')
def folder_exists():
    """Check if a directory exists in an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param object_name: AWS S3 directory name to be searched.
                        If not specified then error
    :return: True if the referenced directory was found, otherwise False
    """
    bucket_name = request.args.get("bucket_name")
    object_name = request.args.get("object_name")

    objects = []
    flag_found = False

    if object_name is None:
        response = jsonify({"status" : 'folder name (object_name) must be provided for search'})
        response.status_code = 400
    else:
        s3 = boto3.client('s3')
        try:
            results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)
            try:
                for i in results['Contents']:
                    objects.append(i['Key'])
            except:
                pass

            if objects !=[]:
                flag_found = True

            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "exists" : flag_found})
            response.status_code = 200

        except ClientError as e:
            response = jsonify({"status" : 'ERROR : check logs'})
            response.status_code = 400

    return response


@app.route('/folders/create')
def create_folder():
    """Create a directory with folder_name under given object_name
    from an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param folder_name: directory to be created under object_name.
                        if object_name=None then directory willl be created
                        at root in the bucket
    :param object_name: AWS S3 directory name. If not specified then same as folder_name
    :return: Directory creation status
    """
    bucket_name = request.args.get("bucket_name")
    object_name = request.args.get("object_name")
    folder_name = request.args.get("folder_name")

    if folder_name is None:
        response = jsonify({"status" : 'folder name must be provided for creation'})
        response.status_code = 400
    else:
        objects = []
        s3 = boto3.client('s3')
        try:
            if object_name:
                object_name = object_name + '/' + folder_name + '/'
            else:
                object_name = folder_name + '/'

            results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)

            try:
                for i in results['Contents']:
                    objects.append(i['Key'])
            except:
                pass

            if objects ==[]:
                s3.put_object(Bucket=bucket_name, Key=object_name)

                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'folder creation successful'})
                response.status_code = 201
            else:
                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'folder already exists'})
                response.status_code = 409

        except Exception as e:
            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'folder creation failed'})
            response.status_code = 400

    return response


@app.route('/folders/delete')
def delete_folder():
    """Delete a directory from an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param object_name: AWS S3 directory
    :return: Directory deletion status
    """
    bucket_name = request.args.get("bucket_name")
    object_name = request.args.get("object_name")

    if object_name is None:
        response = jsonify({"status" : 'folder name must be provided for deletion'})
        response.status_code = 400
    else:
        objects = []
        s3 = boto3.client('s3')
        try:
            if object_name[-1]!='/':
                object_name += '/'

            results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)
            try:
                for i in results['Contents']:
                    objects.append(i['Key'])
            except:
                pass

            if objects !=[]:
                for obj in objects:
                    s3.delete_object(Bucket=bucket_name, Key=obj)

                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'folder deletion successful'})
                response.status_code = 200
            else:
                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'folder does not exists'})
                response.status_code = 404
        except ClientError as e:
            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'folder deletion failed'})
            response.status_code = 400

    return response


@app.route('/files/exists')
def file_exists():
    """Search for a file in an S3 bucket

    :param bucket_name: AWS S3 Bucket
    :param file_name: File to be searched
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if the referenced object was deleted, otherwise False
    """
    bucket_name = request.args.get("bucket_name")
    file_name   = request.args.get("file_name")
    object_name = request.args.get("object_name")

    objects = []
    flag_found = False

    if file_name is None:
        response = jsonify({"status" : 'file name must be provided for search'})
        response.status_code = 400
    else:
        s3 = boto3.client('s3')
        try:
            # If S3 object_name was not specified, use file_name
            if object_name:
                object_name += '/' + file_name
            else:
                object_name = file_name

            results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)

            try:
                for i in results['Contents']:
                    objects.append(i['Key'])
            except:
                pass

            if objects !=[]:
                flag_found = True

            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "exists" : flag_found})
            response.status_code = 200

        except ClientError as e:            # logging.error(e)
            response = jsonify({"status" : 'ERROR : check logs'})
            response.status_code = 400

    return response


@app.route('/files/delete')
def delete_file():
    """Delete an file from an S3 bucket

    :param bucket_name: AWS S3 Bucket
    :param file_name: File to be deleted
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if the referenced object was deleted, otherwise False
    """
    bucket_name = request.args.get("bucket_name")
    file_name   = request.args.get("file_name")
    object_name = request.args.get("object_name")

    if file_name is None:
        response = jsonify({"status" : 'file name must be provided for deletion'})
        response.status_code = 400
    else:
        s3 = boto3.client('s3')
        try:
            # If S3 object_name was not specified, use file_name
            if object_name:
                object_name += '/' + file_name
            else:
                object_name = file_name

            objects = []
            results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)

            try:
                for i in results['Contents']:
                    objects.append(i['Key'])
            except:
                pass
            if objects != []:
                s3.delete_object(Bucket=bucket_name, Key=object_name)

                objects_post = []
                results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)

                try:
                    for i in results['Contents']:
                        objects_post.append(i['Key'])
                except:
                    pass

                if objects_post !=[]:
                    response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file deletion failed'})
                    response.status_code = 400
                else:
                    response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file deletion successful'})
                    response.status_code = 200
            else:
                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file does not exists'})
                response.status_code = 404
        except ClientError as e:
            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file deletion failed'})
            response.status_code = 400

    return response


@app.route('/files/download')
def download_file():
    """Download a file from an S3 bucket

    :param bucket_name: Bucket to upload to
    :param dir: Local directory to save the file
    :param file_name: File to download
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if file was downloaded, else False
    """
    bucket_name = request.args.get("bucket_name")
    file_name   = request.args.get("file_name")
    object_name = request.args.get("object_name")
    dir         = request.args.get("dir")

    if file_name is None:
        response = jsonify({"status" : 'file name must be provided for download'})
        response.status_code = 400
    else:
        s3 = boto3.resource('s3')
        try:
            # If S3 object_name was not specified, use file_name
            if object_name:
                object_name += '/' + file_name
            else:
                object_name = file_name

            if dir:
                if not os.path.exists(dir):
                    os.makedirs(dir)

                file_name = dir + '/'+ file_name

            try:
                s3.Bucket(bucket_name).download_file(object_name, file_name)

                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file download successful'})
                response.status_code = 200

            except ClientError as e:
                if e.response['Error']['Code'] == "404":

                    response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'The object does not exist'})
                    response.status_code = 404
                else:
                    response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file download failed'})
                    response.status_code = 400

        except ClientError as e:
            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file download failed'})
            response.status_code = 400

    return response


@app.route('/files/upload')
def upload_file():
    """Upload a file to an S3 bucket

    :param bucket_name: Bucket to upload to
    :param dir: Local directory from where to file to read for upload
    :param file_name: File to upload
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if file was uploaded, else False
    """
    bucket_name = request.args.get("bucket_name")
    file_name   = request.args.get("file_name")
    object_name = request.args.get("object_name")
    dir         = request.args.get("dir")

    if file_name is None:
        response = jsonify({"status" : 'file name must be provided for upload'})
        response.status_code = 400
    else:
        s3 = boto3.client('s3')
        try:
            # If S3 object_name was not specified, use file_name
            if object_name:
                object_name += '/' + file_name
            else:
                object_name = file_name

            if dir:
                file_name = dir + '/'+ file_name

            s3 = boto3.client('s3')
            try:
                s3.upload_file(file_name, bucket_name, object_name)
                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file upload successful'})
                response.status_code = 200

            except ClientError as e:
                response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file upload failed'})
                response.status_code = 400

        except ClientError as e:
            response = jsonify({"bucket_name": bucket_name, "object_name" : object_name, "status" : 'file upload failed'})
            response.status_code = 400

    return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5004, debug=True)
