import boto3
import configparser
import os

S3_BUCKET = 'kamesh-capstone'
KEY_PATH = "udacitydataset"

def uploadToS3(s3Client,f_name,s3_f_name):
    try:
        s3Client.upload_file(f_name, S3_BUCKET, KEY_PATH + s3_f_name)
    except Exception as e:
        print(e)

def uploadImmigrationDataS3(s3Client):
    root = "/home/workspace/data/18-83510-I94-Data-2016/"
    files = [root + f for f in os.listdir(root)]
    for f in files:
        uploadToS3(s3Client,f,"/raw/i94_immigration_data/18-83510-I94-Data-2016/" + f.split("/")[-1])


def uploadDemographics(s3Client):
    uploadToS3(s3Client,"us-cities-demographics.csv","/raw/demographics/us-cities-demographics.csv")


def uploadGlobalTemperatures(s3Client):
    root = "/home/workspace/data2/"
    files = [root + f for f in os.listdir(root)]
    for f in files:
        uploadToS3(s3Client,f,"/raw/globaltemperatures/" + f.split("/")[-1])


def uploadAirportCode(s3Client):
    uploadToS3(s3Client,"airport-codes_csv.csv", "/raw/airportcode/airport-codes_csv.csv")

def uploadCodes(s3Client):
    uploadToS3(s3Client,"./dictionary_data/i94addrl.txt", "/raw/codes/i94addrl.txt")
    uploadToS3(s3Client,"./dictionary_data/i94cntyl.txt", "/raw/codes/i94cntyl.txt")
    uploadToS3(s3Client,"./dictionary_data/i94prtl.txt", "/raw/codes/i94prtl.txt")
    uploadToS3(s3Client,"./dictionary_data/i94model.txt", "/raw/codes/i94model.txt")
    uploadToS3(s3Client,"./dictionary_data/i94visa.txt", "/raw/codes/i94visa.txt")


def main():


    # Get session
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
    os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
    s3Client = boto3.client('s3')
    
    uploadImmigrationDataS3(s3Client)
    uploadDemographics(s3Client)
    uploadGlobalTemperatures(s3Client)
    uploadAirportCode(s3Client)
    uploadCodes(s3Client)


if __name__ == "__main__":
    main()