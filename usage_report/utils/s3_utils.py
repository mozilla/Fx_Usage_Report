import boto3
import json


def file_exists(bucket_name, filename, aws_access_key_id=None, aws_secret_access_key=None):
    """ check if a file exists in S3
    params: bucket_name, str, name of bucket
    filename, str, name of file (prefix + file name)
    aws_access_key_id, aws_secret_access_key, if None it should check env
    return: True if file exists
    """
    s3 = boto3.Session(aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key).resource('s3')
    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=filename))
    if len(objs) > 0 and objs[0].key == filename:
        return True
    else:
        return False


def read_from_s3(bucket_name, filename, aws_access_key_id=None, aws_secret_access_key=None):
    """ read JSON from s3
    params: bucket_name, str, name of bucket
    filename, str, name of file (prefix + file name)
    return: JSON as dict, None if file doesn't exist in S3
    """
    if file_exists(bucket_name, filename, aws_access_key_id, aws_secret_access_key):
        s3 = boto3.Session(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key).resource('s3')
        content_object = s3.Object(bucket_name, filename)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return json.loads(file_content)


def write_to_s3(bucket_name, filename, data, aws_access_key_id=None, aws_secret_access_key=None,
                acl='public-read'):
    """ write dict as JSON to s3
    params: bucket_name, str, name of bucket
    filename, str, name of file (prefix + file name)
    return: nothing
    """
    s3 = boto3.Session(aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key).resource('s3')
    obj = s3.Object(bucket_name, filename)
    obj.put(Body=json.dumps(data, ensure_ascii=False).encode('utf8'), ACL=acl)
