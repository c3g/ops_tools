#!/usr/bin/env python3
import argparse
import configparser
import datetime
import re

import boto3

def get_config(rclone_config_file, c_section):

    config = configparser.ConfigParser()
    config.read(rclone_config_file)
    section = config[c_section]
    access_key_id = section['access_key_id']
    secret_access_key = section['secret_access_key']
    endpoint = section['endpoint']
    return (access_key_id,
            secret_access_key, 
            endpoint)

def main(access_key_id, secret_access_key, endpoint, bucket_name, dry_run=False):
    """
    Main entry point
    :param rclone_config_file:
    :param bucket_name:
    :param dry_run:
    :return:
    """

    session = boto3.Session(
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    )
    s3 = session.resource('s3', endpoint_url=endpoint)

    cleanup_backup(s3, bucket_name, dry_run=dry_run)

def cleanup_backup(s3, bucket_name, dry_run=False):
    """
    Cleanup backup: keep last week, every Sunday of the last two
    month and then the first sunday of every month for the past two years
    :param s3:
    :param bucket_name:
    :param dry_run:
    :return:
    """
    regex = re.compile(r'.*[_.](\d{4}-\d{2}-\d{2})\.(sql\.dump|tar)')

    now = datetime.datetime.now()
    ten_days = now - datetime.timedelta(days=10)
    last_two_months = now - datetime.timedelta(weeks=8)
    last_two_years = now - datetime.timedelta(weeks=104)
    sunday = 6

    keep = []
    to_delete = []
    for obj in s3.Bucket(bucket_name).objects.all():
        try :
           time_str = regex.search(obj.key).groups()[0]
        except AttributeError:
           print(f'No time stamp in {obj.key}')
           keep.append(obj.key)
           continue
        ts = datetime.datetime.strptime(time_str, '%Y-%m-%d')
        # If the timestamp is from the last week
        if (ts > ten_days or  # last ten days
        ts > last_two_months and ts.weekday() == sunday or  # every sunday for two months
        ts > last_two_years and ts.weekday() == sunday and ts.day < 8):  # First sunday of the month of last two years
            keep.append(obj.key)
        else:
            to_delete.append({"Key": obj.key})

    print(f"To keep")
    for k in keep:
        print(k)
    print("\nTo delete {}".format('\n'.join([ list(e.values())[0] for e in to_delete])))
    
    if dry_run is True:
        print("Dry run, not deleting")
    else:
        print("Deleting old backups")
        s3.meta.client.delete_objects(Bucket=bucket_name, Delete={'Objects': to_delete})
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Cleanup c3g production DB backups, note that it is a dry run by default!')
    parser.add_argument('--id', help='S3 id')
    parser.add_argument('--bucket', help='bucket name', default="DB_backups")
    parser.add_argument('--secret', help='S3 secret')
    parser.add_argument('--rclone', help='rclone config file path can be provided instead of the id and secret themselves')
    parser.add_argument('--config', help='rclone config section', default="c3g-prod")
    parser.add_argument('--endpoint', help='s3 enpoint url')
    parser.add_argument('-r', '--not-dry-run', action='store_true', help='Run the cleanup')

    args = parser.parse_args()
    dry_run = True
    if args.not_dry_run:
        dry_run = False

    rclone = args.rclone
    if rclone:
        c_section = args.config
        access_key_id, secret_access_key, endpoint = get_config(rclone, c_section)

    if args.endpoint:
        endpoint = args.endpoint
    if args.id:
        access_key_id = args.id
    if args.secret:
        secret_access_key = args.secret
    if args.bucket:
        bucket_name = args.bucket


    main(access_key_id, secret_access_key, endpoint, bucket_name, dry_run=dry_run)
