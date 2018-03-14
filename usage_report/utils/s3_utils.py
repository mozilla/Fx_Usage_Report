import boto
import json
from boto.s3.key import Key


def filename_from_date(filename, date):
    chunks = filename.split('.')
    return chunks[0] + '_' + date + '.' + '.'.join(chunks[1:])


def read_from_s3(bucket_name, filename, aws_access_key_id=None, aws_secret_access_key=None):
    """ Reads a json file from s3.

    Parameters:
    bucket_name - The name of the bucket on s3
    filename - The name of the json file in the bucket on s3
    aws_access_key_id - Authentication token if needed. If None it uses the
                        enviorment variable AWS_ACCESS_KEY_ID.
    aws_secret_access_key - Authentication token if needed. If None it uses the
                        enviorment variable AWS_SECRET_ACCESS_KEY.

    Returns:
    A dictionary or list with the contents of the json file on s3.

    """
    connection = boto.connect_s3(aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key,
                                 host='s3-external-2.amazonaws.com') # requried for boto on emr

    bucket = connection.get_bucket(bucket_name)
    our_json = bucket.get_key(filename)

    if our_json:
        return json.load(our_json)


def write_to_s3(bucket_name, filename, d, aws_access_key_id=None, aws_secret_access_key=None):
    """ Write a dictionary to s3 as a json.

    Parameters:
    bucket_name - The name of the bucket on s3
    filename - The name of the file to write to in the bucket on s3.
    d - The dictionary you want to put as a json on s3.
    aws_access_key_id - Authentication token if needed. If None it uses the
                        enviorment variable AWS_ACCESS_KEY_ID.
    aws_secret_access_key - Authentication token if needed. If None it uses the
                        enviorment variable AWS_SECRET_ACCESS_KEY.
    """
    connection = boto.connect_s3(aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key,
                                 host='s3-external-2.amazonaws.com') # requried for boto on emr

    bucket = connection.get_bucket(bucket_name)
    key = Key(bucket, filename)

    key.set_contents_from_string(json.dumps(d))
