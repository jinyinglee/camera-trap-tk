# 2022 04 based on new database schema
# This is script for importing images & annotation directly from hard disk drive
# note: did not consider substudyarea
from src.image import ImageManager
import requests
from datetime import datetime
import bson
import pytz
import pandas as pd
import hashlib
from pathlib import Path

from PIL import Image as PILImage
from PIL import ExifTags
from PIL import TiffImagePlugin
from pandas.io.formats.format import Datetime64Formatter
import json
import numpy as np

import logging
import boto3
from botocore.exceptions import ClientError
import os
import psycopg2
from ast import literal_eval


THUMB_MAP = (
    ('q', (75, 75)),
    ('m', (500, 500)),
    ('l', (1024, 1024)),
    ('x', (2048, 2048)),
)


def make_thumb(src_path, thumb_source_path, oid):
    for i in THUMB_MAP:
        # stem = Path(src_path).stem
        target_filename = f'{oid}-{i[0]}.jpg'
        target_path = thumb_source_path.joinpath(Path(target_filename))
        #print (source_path, target_path)
        thumb = PILImage.open(src_path)
        thumb.thumbnail(i[1], PILImage.ANTIALIAS)
        if thumb.mode != 'RGB':  # RGBA, P?
            thumb = thumb.convert('RGB')
        thumb.save(target_path, "JPEG")


def upload_to_s3(file_path, object_name):
    key = ''
    secret = ''
    bucket_name = 'camera-trap-21-prod'
    ret = {
        'data': {},
        'error': ''
    }
    s3_client = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    try:
        response = s3_client.upload_file(
            file_path,
            bucket_name,
            object_name,
            ExtraArgs={'ACL': 'public-read'}
        )
        ret['data'] = response
    except ClientError as e:
        # logging.error(e)
        print('s3 upload error', e)
        ret['error'] = 's3 upload client error'
    except S3UploadFailedError as e:
        print(e)
        ret['error'] = 's3 upload failed'
    return ret


def get_image_info(path):
    utc_tz = pytz.timezone('UTC')
    x = Path(path)
    image_hash, coverted_dt, exif = '', '', '{}'
    try:
        if str(x)[-3:].lower() not in ['avi', 'mp4']:
            img_manager = ImageManager(x)
            exif = img_manager.get_exif()
            image_hash = img_manager.make_hash()
            dtime = exif.get('DateTime', '')
            if dtime:
                dt = datetime.strptime(exif.get('DateTime', ''), '%Y:%m:%d %H:%M:%S')
                timestamp = dt.timestamp()
                coverted_dt = datetime.fromtimestamp(timestamp, utc_tz)
            else:
                stat = img_manager.get_stat()
                timestamp = int(stat.st_mtime)
                coverted_dt = datetime.fromtimestamp(timestamp, utc_tz)
    except:
        pass
    return [image_hash, coverted_dt, exif]


project_id = 288

detail_url = f"https://dbtest.camera-trap.tw/api/client/v1/projects/{project_id}"
r = requests.get(detail_url)
details = r.json()['studyareas']
details = pd.DataFrame(details)
details = details.explode('deployments')
details['d_name'] = details['deployments'].apply(lambda x: x.get('name'))
details['d_id'] = details['deployments'].apply(lambda x: x.get('deployment_id'))
details = details[['studyarea_id', 'name', 'd_name', 'd_id']]

location_map = {
    '新竹': 'HC',
    '嘉義': 'CY',
    '羅東': 'LD',
    '南投': 'NT',
    '東勢': 'DS'
}

record_data_length = []

for location in ['新竹', '嘉義', '羅東', '南投', '東勢']:
    df = pd.read_excel(f'/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/2020-09~/文字檔/{location}處.xlsx')
    # len(df)
    # len(df.檔名.unique())
    # 台東 287381 屏東 96920 花蓮 126167
    df['objectID'] = ''
    obj_df = df.groupby(['檔名'], as_index=False)['objectID'].apply(lambda x: bson.objectid.ObjectId())
    df = pd.merge(df.drop(columns=['objectID']), obj_df, on="檔名", how="left")
    # len(df.objectID.unique())
    record_data_length += [(location, len(df), len(df.檔名.unique()), len(df.objectID.unique()))]
    # remove leadind & trailing white space
    df['物種'] = df['物種'].str.strip()
    df['檔名_mac'] = df['檔名'].replace('F\:', '/Volumes/Transcend', regex=True).replace('\\\\', '/', regex=True)
    df.to_csv(f'/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/2020-09~/{location}-edited.csv', index=False)
    # df = pd.read_csv('/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/2020-09~/台東處-edited.csv')
    df = df.replace({np.nan: ''})
    df['image_hash'] = ''
    df['datetime'] = ''
    df['exif'] = ''
    for i in df.index:
        if i % 100 == 0 and i != 0:
            print(location, i)
        df.loc[i, ['image_hash', 'datetime', 'exif']] = get_image_info(df.iloc[i].檔名_mac)
    df['filename'] = df['檔名'].apply(lambda x: x.split('\\')[-1])
    df['folder_name'] = df['檔名'].apply(lambda x: x.split('\\')[-2])
    df = df.rename(columns={'樣區': 'name', '相機位置': 'd_name'})
    df = df.merge(details)
    csv_df = df[['filename', 'folder_name', '物種', '性別', '年齡', '備註', 'objectID', 'studyarea_id', 'd_id', 'image_hash', 'datetime']]
    csv_df = csv_df.rename(columns={'檔名': 'filename', '物種': 'species', '性別': 'sex', '年齡': 'life_stage', '備註': 'remarks',
                                    'objectID': 'image_uuid', 'd_id': 'deployment_id'})
    csv_df['memo'] = '2020-09-GJW'
    csv_df['project_id'] = 288
    csv_df['file_url'] = csv_df['image_uuid'].apply(lambda x: str(x) + '-m.jpg')
    csv_df = csv_df[['memo', 'project_id', 'studyarea_id', 'deployment_id', 'filename', 'datetime', 'species',
                    'life_stage', 'sex', 'remarks', 'image_uuid', 'image_hash', 'file_url', 'folder_name']]
    csv_df.to_csv(f'/Users/taibif/Documents/GitHub/ct22-volumes/bucket/{location_map[location]}.csv', index=False)
    info_df = df[['objectID', 'exif']]
    info_df = info_df.drop_duplicates()
    info_df['exif'] = info_df['exif'].apply(lambda x: json.dumps(x))
    info_df['exif'] = info_df['exif'].apply(lambda x: x.replace('\\u0000', '').replace('\\u0001', '').replace('\\u0002', '').replace('\\u0003', ''))
    info_df = info_df.rename(columns={'objectID': 'image_uuid'})
    info_df.to_csv(f'/Users/taibif/Documents/GitHub/ct22-volumes/bucket/{location_map[location]}_info.csv', index=False)


# 資料庫id (taicat_image)
# 起始 11202970
# 台東 11572752
# 屏東 11669672


# PSQL NOTES

# copy from csv
"""
COPY taicat_image(memo, project_id, studyarea_id, deployment_id, filename, datetime, species, life_stage, sex,
                  remarks, image_uuid, image_hash, file_url, folder_name)
FROM '/bucket/PT.csv'
DELIMITER ',' CSV HEADER

COPY taicat_image_info(image_uuid, exif)
FROM '/bucket/PT_info.csv'
DELIMITER ',' CSV HEADER
"""

# update default value
"""
SELECT column_name, column_default
FROM information_schema.columns
WHERE(table_schema, table_name) = ('public', 'taicat_image')
ORDER BY ordinal_position

ALTER TABLE taicat_image ALTER COLUMN annotation SET DEFAULT '{}'
ALTER TABLE taicat_image ALTER COLUMN exif SET DEFAULT '{}'
ALTER TABLE taicat_image ALTER COLUMN source_data SET DEFAULT '{}'
ALTER TABLE taicat_image ALTER COLUMN last_updated SET DEFAULT NOW()
ALTER TABLE taicat_image ALTER COLUMN created SET DEFAULT NOW()
ALTER TABLE taicat_image ALTER COLUMN sequence_definition SET DEFAULT ''
ALTER TABLE taicat_image ALTER COLUMN from_mongo SET DEFAULT false
ALTER TABLE taicat_image ALTER COLUMN count SET DEFAULT 1
ALTER TABLE taicat_image ALTER COLUMN image_hash SET DEFAULT ''
alter table taicat_image alter column image_hash drop not null
ALTER TABLE taicat_image_info ALTER COLUMN source_data SET DEFAULT '{}'
"""
