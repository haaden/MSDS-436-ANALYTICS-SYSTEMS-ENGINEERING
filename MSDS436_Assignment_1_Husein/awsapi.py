#***********************************************
# File: module1-awsapi.py
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
#           10. upload csv file
#          11. Download file
#          12. Delete File
#          13. File exists
# Auth: Shreenidhi Bharadwaj modified Husein Adenwala
# Date: 1/15/2022
# ALL RIGHTS RESERVED | DO NOT DISTRIBUTE
#************************************************/
import os
import boto3
import logging
from botocore.exceptions import ClientError
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter

def list_buckets():
    """Connect to S3 and query all buckets

    :return: list of buckets
    """

    s3 = boto3.client('s3')

    # Call S3 to list current buckets
    response = s3.list_buckets()

    # Get a list of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]

    return buckets


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the users S3
    default region read from credentials in ~/.aws/config.

    :param bucket_name: Bucket to create
    :param region: region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    try:
        if region is None:
            session = boto3.Session(profile_name='default')
            region = session.region_name
            location = {'LocationConstraint': region}
            s3_client = session.client('s3')
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
            logging.info('Created bucket {0} in the S3 default region ({1})'.format(bucket_name, region))
        else:
            s3 = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3.create_bucket(Bucket=bucket_name,
                             CreateBucketConfiguration=location)
            logging.info('Created bucket {0} in the S3 region ({1})'.format(bucket_name, region))

    except ClientError as e:
        logging.error(e)
        return False

    return True


def delete_bucket(bucket_name):
    """Delete an empty S3 bucket

    If the bucket is not empty, the operation fails.

    :param bucket_name: AWS S3 bucket
    :return: True if the referenced bucket was deleted, otherwise False
    """

    s3 = boto3.client('s3')
    try:
        s3.delete_bucket(Bucket=bucket_name)
        logging.info('Bucket {0} deleted successfully'.format(bucket_name))

    except ClientError as e:
        logging.error(e)
        return False

    return True


def bucket_exists(bucket_name):
    """Determine whether bucket exists and the user has permission
    to access it

    :param bucket_name: AWS S3 bucket to be searched
    :return: True if the referenced bucket_name exists, otherwise False
    """

    s3 = boto3.client('s3')
    try:
        response = s3.head_bucket(Bucket=bucket_name)
        logging.info('{0} exists and you have permission to access it.'.format(bucket_name))

    except ClientError as e:
        logging.debug(e)
        logging.info('{0} does not exist or you do not have permission to access it.'.format(bucket_name))
        return False

    return True


def list_objects(bucket_name, object_name=None):
    """List all directories & files starting with given object_name from an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param object_name: AWS S3 directory. If not specified then empty string
    :return: List of all the directories & files under given object_name(directory)
    """

    objects = []
    if object_name is None:
        object_name = ''

    s3 = boto3.client('s3')
    try:
        results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)
        try:
            for i in results['Contents']:
                objects.append(i['Key'])
            logging.info('Listing objects under {0} found in {1}'.format(object_name, bucket_name))

        except:
            logging.info('No objects found under {0} found in {1}'.format(object_name, bucket_name))
            pass

    except ClientError as e:
        logging.error(e)

    return objects


def create_folder(bucket_name, folder_name, object_name=None):
    """Create a directory with folder_name under given object_name
    from an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param folder_name: directory to be created under object_name.
                        if object_name=None then directory willl be created
                        at root in the bucket
    :param object_name: AWS S3 directory name. If not specified then same as folder_name
    :return: True if the referenced directory is created, otherwise False
    """

    s3 = boto3.client('s3')
    try:
        if object_name:
            object_name = object_name + folder_name + '/'
        else:
            object_name = folder_name + '/'

        s3.put_object(Bucket=bucket_name, Key=object_name)
        logging.info('Added {0} to {1}'.format(object_name, bucket_name))

    except Exception as e:
        logging.error(e)
        return False

    return True


def delete_folder(bucket_name, object_name):
    """Delete a directory from an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param object_name: AWS S3 directory
    :return: True if the referenced directory was deleted, otherwise False
    """

    objects = []
    if object_name[-1]!='/':
        object_name += '/'

    s3 = boto3.client('s3')
    try:
        results = s3.list_objects(Bucket=bucket_name, Prefix=object_name)
        try:
            for i in results['Contents']:
                objects.append(i['Key'])
        except:
            pass

        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj)

        logging.info('{0} was deleted from {1}'.format(object_name, bucket_name))

    except ClientError as e:
        logging.error(e)
        return False

    return True


def folder_exists(bucket_name, object_name=None):
    """Check if a directory exists in an S3 bucket

    :param bucket_name: AWS S3 bucket
    :param object_name: AWS S3 directory name to be searched.
                        If not specified then error
    :return: True if the referenced directory was found, otherwise False
    """

    objects = []
    flag_found = False

    if object_name is None:
        logging.error('folder name (object_name) must be provided for search'.format(object_name, bucket_name))
        return flag_found

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
                logging.info('{0} found in {1}'.format(object_name, bucket_name))
            else:
                logging.info('{0} not found in {1}'.format(object_name, bucket_name))

        except ClientError as e:
            logging.error(e)

    return flag_found


def upload_file(bucket_name, dir, file_name, object_name=None):
    """Upload a file to an S3 bucket

    :param bucket_name: Bucket to upload to
    :param dir: Local directory from where to file to read for upload
    :param file_name: File to upload
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    file_name = dir + file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        logging.info('File {0} uploaded successfully'.format(file_name))

    except ClientError as e:
        logging.error(e)
        return False

    return True


def upload_csv(bucket_name, key, body):
    """Upload csv file to an S3 bucket

    :param Key:  name for csv files
    ;param Body: data for csv file
    :param bucket_name: Bucket to upload to
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :param file_name: File to upload
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if file was uploaded, else False
    """


    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
        logging.info('File {0} uploaded successfully'.format(key))

    except ClientError as e:
        logging.error(e)
        return False

    return True




def download_file(bucket_name, dir, file_name, object_name=None):
    """Download a file from an S3 bucket

    :param bucket_name: Bucket to upload to
    :param dir: Local directory to save the file
    :param file_name: File to download
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if file was downloaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    if dir:
        if not os.path.exists(dir):
            os.makedirs(dir)

        file_name = dir + file_name

    s3 = boto3.resource('s3')
    try:
        s3.Bucket(bucket_name).download_file(object_name, file_name)
        logging.info('Successfully downloaded {0} from {1}'.format(object_name, bucket_name))

    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.error("The object does not exist.")
        else:
            logging.error(e)
        return False

    return True


def delete_file(bucket_name, file_name, object_name=None):
    """Delete an file from an S3 bucket

    :param bucket_name: AWS S3 Bucket
    :param file_name: File to be deleted
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if the referenced object was deleted, otherwise False
    """

    orig_file_name = file_name
    orig_object_name = object_name

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

    s3 = boto3.client('s3')
    try:
        s3.delete_object(Bucket=bucket_name, Key=object_name)

        if file_exists(bucket_name, orig_file_name, orig_object_name):
            logging.error('Issue deleting {0} from {1}'.format(object_name, bucket_name))
            return False
        else:
            logging.info('{0} was deleted from {1}'.format(object_name, bucket_name))

    except ClientError as e:
        logging.error(e)
        return False

    return True


def file_exists(bucket_name, file_name, object_name=None):
    """Search for a file in an S3 bucket

    :param bucket_name: AWS S3 Bucket
    :param file_name: File to be searched
    :param object_name: AWS S3 directory name. If not specified then same as file_name
    :return: True if the referenced object was deleted, otherwise False
    """

    objects = []
    flag_found = False

    # If S3 object_name was not specified, use file_name
    if object_name:
        object_name += file_name
    else:
        object_name = file_name

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
            logging.info('{0} found in {1}'.format(object_name, bucket_name))
        else:
            logging.info('{0} not found in {1}'.format(object_name, bucket_name))

    except ClientError as e:
        logging.error(e)

    return flag_found


def main():

    parser = ArgumentParser(formatter_class=RawTextHelpFormatter, prog='awsapi.py', description='Connect to AWS S3 & perform various operations. \n ')

    parser.add_argument('-b', '--bucket_name', dest='bucket_name', help='AWS bucket name\n\n', required=False, default=None)
    parser.add_argument('-o', '--object_name', dest='object_name', help='AWS directory name\n\n', required=False, default=None)
    parser.add_argument('-d', '--dir', dest='dir', help='Local directory for downloading files & uploading to AWS\n\n', required=False, default=None)
    parser.add_argument('-f', '--file_name', dest='file_name', help='File name\n\n', required=False, default=None)
    parser.add_argument('-r', '--region', dest='region', help='AWS bucket region to be provided for bucket creation\n\n', required=False, default=None)
    parser.add_argument('-rd', '--remote_dir', dest='remote_dir', help='AWS directory to be created\n\n', required=False, default=None)
    parser.add_argument('-l', '--log_level', dest='log_lvl', choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Logging level to create logs\n\n', default='WARNING')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-bo', '--bucket_operation', dest='bucket_operation', choices=['list', 'create', 'delete', 'exists'], help='Perform bucket operation\n\n')
    group.add_argument('-oo', '--object_operation', dest='object_operation', choices=['list', 'create', 'delete', 'exists'], help='Perform object/folder operation\n\n')
    group.add_argument('-fo', '--file_operation', dest='file_operation', choices=['upload', 'download', 'delete', 'exists'], help='Perform file operation\n\n')

    args = parser.parse_args()

    bucket_name      = args.bucket_name
    object_name      = args.object_name
    dir              = args.dir
    file_name        = args.file_name
    region           = args.region
    remote_dir       = args.remote_dir
    bucket_operation = args.bucket_operation
    object_operation = args.object_operation
    file_operation   = args.file_operation
    log_lvl          = args.log_lvl

    if log_lvl.upper()== 'NOTSET':
        log_level = logging.NOTSET
    elif log_lvl.upper()== 'DEBUG':
        log_level = logging.DEBUG
    elif log_lvl.upper()== 'INFO':
        log_level = logging.INFO
    elif log_lvl.upper()== 'WARNING':
        log_level = logging.WARNING
    elif log_lvl.upper()== 'ERROR':
        log_level = logging.ERROR
    elif log_lvl.upper()== 'CRITICAL':
        log_level = logging.CRITICAL

    # Set up logging
    logging.basicConfig(level=log_level,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    if bucket_operation in ['create', 'delete', 'exists'] and not(bucket_name):
        parser.error('-b BUCKET_NAME is mandatory with -bo {create,delete,exists} only list can be provided without -b BUCKET_NAME')

    if object_operation in ['list', 'exists'] and not(bucket_name):
        parser.error('-b BUCKET_NAME is mandatory -o OBJECT_NAME is optional with -oo {list,create,exists}')
    elif object_operation == 'create' and (not(bucket_name) or not(remote_dir)):
        parser.error('-b BUCKET_NAME -rd REMOTE_DIR are mandatory -o OBJECT_NAME is optional with -oo {create}')
    elif object_operation == 'delete' and (not(bucket_name) or not(object_name)):
        parser.error('-b BUCKET_NAME -o OBJECT_NAME are mandatory with -oo {delete}')

    if file_operation in ['upload', 'download'] and (not(bucket_name) or not(dir) or not(file_name)):
        parser.error('-b BUCKET_NAME -d LOCAL_DIR -f FILE_NAME are mandatory & -o OBJECT_NAME is optional with -fo {upload,download}')
    elif file_operation in ['delete', 'exists'] and (not(bucket_name) or not(file_name)):
        parser.error('-b BUCKET_NAME -f FILE_NAME are mandatory & -o OBJECT_NAME  is optional with -fo {delete,exists}')

    # Add '/' to directory strings if not already present at the end
    if object_name and object_name[-1]!='/':
        object_name += '/'

    if dir and dir[-1]!='/':
        dir += '/'

    print_stmt = None

    if bucket_operation == 'list':

        print_stmt = 'Bucket List: {0}'.format(list_buckets())

    if bucket_operation == 'create':

        if create_bucket(bucket_name, region):
            print_stmt = 'create_bucket operation successful'
        else:
            print_stmt = 'create_bucket operation not successful'

    if bucket_operation == 'delete':

        if delete_bucket(bucket_name):
            print_stmt = 'delete_bucket operation successful'
        else:
            print_stmt = 'delete_bucket operation not successful'

    if bucket_operation == 'exists':

        if bucket_exists(bucket_name):
            print_stmt = 'bucket_exists operation successful'
        else:
            print_stmt = 'bucket not found / no  bucket permission / bucket_exists operation not successful - check above actual cause'

    if object_operation == 'list':

        objects = list_objects(bucket_name, object_name)

        if objects != []:
            print('\n\nObject List: \n')
            for obj in objects:
                print(obj)
            print('\n\n')
        else:
            print_stmt = 'No objects found'

    if object_operation == 'create':

        if create_folder(bucket_name, remote_dir, object_name):
            print_stmt = 'create_folder operation successful'
        else:
            print_stmt = 'create_folder operation not successful'

    if object_operation == 'delete':

        if delete_folder(bucket_name, object_name):
            print_stmt = 'delete_folder operation successful'
        else:
            print_stmt = 'delete_folder operation not successful'

    if object_operation == 'exists':

        if folder_exists(bucket_name, object_name):
            print_stmt = 'folder_exists operation successful'
        else:
            print_stmt = 'folder not found / folder_exists operation not successful - check above actual cause'

    if file_operation == 'upload':

        if upload_file(bucket_name, dir, file_name, object_name):
            print_stmt = 'upload_file operation successful'
        else:
            print_stmt = 'upload_file operation not successful'

    if file_operation == 'download':

        if download_file(bucket_name, dir, file_name, object_name):
            print_stmt = 'download_file operation successful'
        else:
            print_stmt = 'download_file operation not successful'

    if file_operation == 'delete':

        if delete_file(bucket_name, file_name, object_name):
            print_stmt = 'delete_file operation successful'
        else:
            print_stmt = 'delete_file operation not successful'

    if file_operation == 'exists':

        if file_exists(bucket_name, file_name, object_name):
            print_stmt = 'file_exists operation successful'
        else:
            print_stmt = 'file not found / file_exists operation not successful - check above actual cause'

    if print_stmt:
        print('\n\n' + print_stmt + '\n\n')

if __name__ == '__main__':
    main()
