import io
from datetime import datetime

import boto3
import requests
from PIL import Image

import logging
import time
import threading
import traceback

import PIL
import os
import glob

# ==============================================
# =============== Configurations ===============
# ==============================================

# configure S3 Bucket
s3 = boto3.resource('s3')

# Retrieve the list of existing buckets
s3_client = boto3.client('s3')
resp = s3_client.list_buckets()

s3_bucketList = []
DataFileList = []

# Output the bucket names
for bucket in resp['Buckets']:
    s3_bucketList.append(bucket["Name"])

try:
    DataFileList = os.listdir('data/')
except Exception as e:
    print('Error :: Faild to load image list')

# Time Configuration
runningTime = datetime.now()

# PIL Configd
Image.MAX_IMAGE_PIXELS = None
IMG_QUALITY = 30

# Logging Config
log_file_name = time.strftime("%m%d%Y-%H:%M:%S", time.localtime())

logging.basicConfig(
    level=logging.INFO,
    filename=f"logs/image_convert_log_{log_file_name}.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Bucket Config
BUCKET_NAME = "cog-prod"
FOLDER_PREFIX = ""
DATA_FILE_NAME = "Prod.dat"

# threadding
THREAD_COUNT = 10
THREAD_INTERVAL = 1



# ==============================================
# =============== Image Optimize ===============
# ==============================================
def optimizeImage(uri):

    logging.info(f'Fetching : {uri} ')

    bimgdata = io.BytesIO()

    print(f'Downloading : {BUCKET_NAME}/{uri}')

    s3.Bucket(BUCKET_NAME).download_fileobj(uri, bimgdata)

    bimgdata.seek(0)
    img = Image.open(bimgdata)
    imgData = io.BytesIO()

    # Save to in memory
    img.save(imgData, 'webp', optimize=True, quality=IMG_QUALITY)
    imgData.seek(0)


    newkey = ""

    if str(uri).endswith(".png"):
        newkey = uri.replace('.png', '.webp')

    if str(uri).endswith(".jpg"):
        newkey = uri.replace('.jpg', '.webp')

    if str(uri).endswith(".jpeg"):
        newkey = uri.replace('.jpeg', '.webp')

    print(f'Uploading : {newkey}')

    s3.Bucket(BUCKET_NAME).put_object(Key=newkey, Body=imgData, ContentType='image/webp')
    logging.info(f'Uploaded : {newkey} ')
    print(f'Uploaded : {newkey} ')


# =================================================
# =============== Create Image List ===============
# =================================================
def createImageList():

    dataFile = open(f'data/{DATA_FILE_NAME}', 'a')
    count = 0
    objSize = 0

    print(f'Fetching Keys from Bucket :: {BUCKET_NAME}')
    logging.info(f'Fetching Keys from Bucket :: {BUCKET_NAME}')

    for obj in s3.Bucket(BUCKET_NAME).objects.all():

        if len(FOLDER_PREFIX) > 0:

            if str(obj.key).startswith(FOLDER_PREFIX) and (str(obj.key).endswith(".png") or str(obj.key).endswith(".jpg") or str(obj.key).endswith(".jpeg")):

                count += 1
                objSize += obj.size
                dataFile.write(f'{obj.key}\n')
        
        else:

            if str(obj.key).endswith(".png") or str(obj.key).endswith(".jpg") or str(obj.key).endswith(".jpeg"):

                count += 1
                objSize += obj.size
                dataFile.write(f'{obj.key}\n')
    
    dataFile.close()
    print(f'Total Object Count  : {count}')
    print(f'Total Object Size   : {objSize / (1024 * 1024)}MB')
    print(f'Data File Saved in  : data/{DATA_FILE_NAME}')

    logging.info(f'Total Object Count  : {count}')
    logging.info(f'Total Object Size   : {objSize / (1024 * 1024)}MB')
    logging.info(f'Data File Saved in  : data/{DATA_FILE_NAME}')

# =======================================================
# =============== Loop through Image List ===============
# =======================================================
def loopUrls():

    with open(f'data/{DATA_FILE_NAME}', "r") as file:
        objects = file.readlines()

        try:
            for uri in objects:
                while len(threading.enumerate()) > THREAD_COUNT:
                    pass
                thread1 = threading.Thread(
                    target=optimizeImage,
                    kwargs={"uri": uri.replace("\n", "")}
                )
                thread1.start()
                time.sleep(THREAD_INTERVAL)
        except Exception as e:
            print(traceback.print_exc())

# ================================================ 
# =============== List All Buckets =============== 
# ================================================
def listAllBuckets():
    print ("")
    print ("All Availabale Buckets")
    print ("")

    for i,bucket in enumerate(s3_bucketList):
        print(f'\t{i} - {bucket}')

    print ("")

# =================================================== 
# =============== List All Data Files =============== 
# ===================================================
def listAllDataFiles():
    print ("")
    print ("All Availabale Image lists")
    print ("")

    for i,fName in enumerate(DataFileList):
        print(f'\t{i} - {fName}')

    print ("")

# ===============================================
# =============== Get Bucket Name ===============
# ===============================================
def getBucketName():

    success = False

    while not success:

        print("")
        bn = input(f'Enter the bucket id (eg : 0) : ')
        print("")

        if len(bn) > 0 and int(bn) > -1 and int(bn) < len(s3_bucketList):
            global BUCKET_NAME
            BUCKET_NAME = s3_bucketList[int(bn)]
            print(f"Target bucket set to : {BUCKET_NAME}")
            print("")
            success = True
        
        else:
            print("Error :: Invalid Bucket ID")

# ==================================================
# =============== Get Data File Name ===============
# ==================================================
def getDataFileName():

    success = False

    while not success:

        print("")
        bn = input(f'Enter the List id (eg : 0) : ')
        print("")

        if len(bn) > 0 and int(bn) > -1 and int(bn) < len(DataFileList):
            global DATA_FILE_NAME
            DATA_FILE_NAME = DataFileList[int(bn)]
            print(f"Target File set to : {DATA_FILE_NAME}")
            print("")
            success = True
        
        else:
            print("Error :: Invalid File ID")


# ================================================
# =============== Main Entry Point ===============
# ================================================

# Print logo
logo = open("logo.txt", "r") 
print (logo.read())
print ("")
print ("")
print ("\t1  :  Convert single image")
print ("\t2  :  create image list")
print ("\t3  :  convert image list")
print ("")

ui = input(f'Select an option : ')

if int(ui) == 1:
    listAllBuckets()
    getBucketName()

    print("")
    IQ = input(f'Enter the image quality (default: {IMG_QUALITY}) : ')

    if len(IQ) > 0 and int(IQ) > 0:
        IMG_QUALITY = int(IQ)


    objKey = input('Enter the object key (eg: assets/image1.png) : ')

    try:
        optimizeImage(str(objKey))
    except Exception as e:
        print('Error :: Invalid Object key')
        
elif int(ui) == 2:

    listAllBuckets()
    getBucketName()

    fPreFix = input('Enter the objeck key prefix to filter images (eg: assets/) : ')
    
    if len(fPreFix) > 0:
        FOLDER_PREFIX = fPreFix
        print(f"Object key prfix set to : {FOLDER_PREFIX}")
    else:
        print(f"No prefix set, Fetching all files")
    
    print("")

    fileToWrite = input('Enter the filename to write images list : data/')
    
    if len(fileToWrite) > 0:
        DATA_FILE_NAME = fileToWrite
        print(f"File path set to : data/{DATA_FILE_NAME}")
    else:
        DATA_FILE_NAME = f'{BUCKET_NAME}_{FOLDER_PREFIX.replace("/","_")}_{runningTime.strftime("%Y%m%d%H%M%S")}.dat'
        print(f"File path set to : data/{DATA_FILE_NAME}")

    try:
        createImageList()
    except Exception as e:
        print('Error :: Faild to create image list')


elif int(ui) == 3:
    listAllBuckets()
    getBucketName()

    listAllDataFiles()
    getDataFileName()

    print("")
    IQ = input(f'Enter the image quality (default: {IMG_QUALITY}) : ')
    
    if len(IQ) > 0 and int(IQ) > 0:
        IMG_QUALITY = int(IQ)

    print("")
    print(f"Thread count set to : {THREAD_COUNT}")

    print("")
    TC = input(f'Enter the thread count (default: {THREAD_COUNT}) : ')
    
    if len(TC) > 0 and int(tc) > 0:
        THREAD_COUNT = int(TC)

    print("")
    print(f"Thread count set to : {THREAD_COUNT}")

    TI = input(f'Enter the thread interval (default: {THREAD_INTERVAL}) : ')
    
    if len(TI) > 0 and int(TI):
        THREAD_INTERVAL = int(TI)

    print(f"Thread interval set to : {THREAD_INTERVAL}")
    print("")
    print("")

    try:
        loopUrls()
    except Exception as e:
        print('Error :: Faild to convert image list')

else:
    pass
