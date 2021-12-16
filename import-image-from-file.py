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
        thumb.thumbnail(i[1] , PILImage.ANTIALIAS)
        if thumb.mode != 'RGB': # RGBA, P?
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
        #logging.error(e)
        print ('s3 upload error', e)
        ret['error'] = 's3 upload client error'
    except S3UploadFailedError as e:
        print (e)
        ret['error'] = 's3 upload failed'
    return ret


# ---- start ---- #
utc_tz = pytz.timezone('UTC')
df = pd.read_excel('/Users/taibif/Downloads/嘉義處.xlsx')  
df = df.drop_duplicates()
df = df.drop(x)
df.to_csv('/Users/taibif/Downloads/嘉義處_remove_duplicated.csv')
# 台東 187348
# 羅東 48492


# create object id for each file
# ! 確定是否一張照片是一筆資料
len(df) # 105130
len(df.檔名.unique()) # 104712

# g = df.groupby('檔名')
# obj_df = g.apply(lambda x: bson.objectid.ObjectId())
# obj_df = pd.DataFrame(obj_df, columns=['檔名','objectID'])
df['objectID'] = ''
obj_df = df.groupby(['檔名'],as_index=False)['objectID'].apply(lambda x: bson.objectid.ObjectId())

df = pd.merge(df.drop(columns=['objectID']), obj_df, on="檔名", how="left")

# remove leadind & trailing white space
df['物種'] = df['物種'].str.rstrip()
df['物種'] = df['物種'].str.lstrip()

# correct image file path
df['檔名_mac'] = df['檔名'].replace(
    'F\:', '/Volumes/林務局自動相機動物監測整合計畫', regex=True).replace('\\\\', '/', regex=True)

# save excel for mapping thumb & file_url
df.to_csv('/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/csv/羅東處-edited.csv')

# new version-----
for j in df[['objectID', '檔名_mac']].drop_duplicates().index:
    p = Path(df.檔名_mac[j])
    thumb_p = Path('/Volumes/TaiBIF/羅東處')
    oid = df.objectID[j]
    try:
        make_thumb(p, thumb_p, oid)
    except:
        pass
    if j % 100 == 0:
        print(j)
# new version-----

# make thumb
for j in df.index:
    # if j > 34500:
    p = Path(df.檔名_mac[j])
    thumb_p = Path('/Volumes/TaiBIF/羅東處')
    oid = df.objectID[j]
    try:
        make_thumb(p, thumb_p, oid)
    except:
        pass
    if j % 100 == 0:
        print(j)

# upload to S3
thumb_p = '/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/thumbnails/台東處'
image_list = os.listdir(thumb_p) # 748573
count = 0 
for f in image_list:
    count += 1
    if count > 88482:
        upload_to_s3(os.path.join(thumb_p,f),f)
    if count % 1000 == 0:
        print(count)

# delete duplicates on S3




# ----- upload annotation ---- #
connection = psycopg2.connect(user="postgres",
                                password="",
                                host="127.0.0.1",
                                port="5432",
                                database="taicat")


# find current project
# projects_url = "https://dev.camera-trap.tw/api/client/v1/projects/"
# r = requests.get(projects_url)
# projects = r.json()['results']
# projects = pd.DataFrame(projects)

# project_name = "自動相機動物監測整合計畫"
# project_id = projects[projects['name'] == project_name].project_id.values[0]
project_id = 288

# detail_url = f"https://dev.camera-trap.tw/api/client/v1/projects/{project_id}"
# r = requests.get(detail_url)
# details = r.json()['studyareas']
# details = pd.DataFrame(details)

details = pd.read_csv('/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/details.csv')

df = pd.read_csv('/Users/taibif/Documents/01-camera-trap/2020自動相機動物監測整合資料/台東處-edited.csv')
df = df.replace({np.nan: None})

body_list = []
for i in df[['objectID']].drop_duplicates().index:
    if i % 1000 == 0:
        print(i)
    # for i in df.index:
    studyarea_id = details[details['name'] == df.樣區[i]].studyarea_id.values[0]
    tmp_df = pd.DataFrame(details[details['name'] == df.樣區[i]].deployments.values[0])
    deployment_id = tmp_df[tmp_df['name'] ==df.相機位置[i]].deployment_id.values[0]
    filename = df.檔名[i].split('\\')[-1]
    file_path = df.檔名[i]
    x = Path(df.檔名_mac[i])
    img_manager = ImageManager(x)
    exif = img_manager.get_exif()
    image_hash = img_manager.make_hash()
    dtime = exif.get('DateTime', '')
    if dtime:
        dt = datetime.strptime(exif.get('DateTime', ''), '%Y:%m:%d %H:%M:%S')
        timestamp = dt.timestamp()
    else:
        stat = img_manager.get_stat()
        timestamp = int(stat.st_mtime)
    # combine annotations
    obj_id = df.objectID[i]
    annotation_list = []
    for d in df[df['objectID']==obj_id].index:
        annotation = {"species": df.物種[d], "sex": df.性別[d],
                    "remarks": df.備註[d], "age": df.年齡[d]}
        annotation_list += [annotation]
    file_url = f"{df.objectID[i]}-m.jpg"  # -m.jpg結尾
    body = {"project_id": int(project_id),
            "studyarea_id": int(studyarea_id),
            "deployment_id": int(deployment_id),
            "filename": filename,
            "datetime": timestamp,
            "annotation": annotation_list,
            "image_hash": image_hash,
            "file_url": file_url,
            "file_path": file_path,
            "exif": json.dumps(exif)}
    body_list += [body]
    # 每次傳送100筆
    if i % 100 == 0 and i != 0:
        print(i)
        post_url = "http://127.0.0.1:8000/api/hdd/v1/image/annotation/"
        resp = requests.post(post_url, json=body_list)
        print(len(body_list))
        body_list = []



# 先上傳annotation再上傳影像
f = open(Path(
    '\\Volumes\\林務局自動相機動物監測整合計畫\\無壓縮\\台東處\\TD02A-20200401-20200430\\IMG_0001.JPG'))


# upload to s3: camera-trap-21-prod
