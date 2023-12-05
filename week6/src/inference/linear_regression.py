from sklearn.linear_model import LinearRegression
import pickle
from inference.base import Predictor
import boto3
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(
    find_dotenv(filename='../../.env'),
)

# get s3 config from env
Bucket = os.environ["S3_BUCKET_NAME"]
ClientAccessKey = os.environ["S3_ACCESS_KEY"]
ClientSecret = os.environ["S3_SECRET"]
ConnectionUrl = os.environ["S3_ENDPOINT"]
# TODO: change this to your own domain (add domain to cloudflare)
PublicUrl = os.environ["S3_PUBLIC_URL"]

class LinearRegressionModel(Predictor):
    def __init__(self):
        self.s3 = boto3.client(
            service_name ="s3",
            endpoint_url = ConnectionUrl,
            aws_access_key_id = ClientAccessKey,
            aws_secret_access_key = ClientSecret,
            region_name="apac", # Must be one of: wnam, enam, weur, eeur, apac, auto
        )
        self.download_model()
    
    def load_model(self, model_path: str):
        self.model = pickle.dumps(model_path)
        
    def predict(self, input):
        return self.model.predict(input)

    def download_model(self):
        # get lastest model from s3
        objects = self.s3.list_objects_v2(
			Bucket=Bucket,
			Prefix='',
		)
        latest_model = objects['Contents'][0]['Key']
        self.s3.download_file(Bucket, latest_model, latest_model)
        self.model = pickle.load(open(latest_model, 'rb'))
        return latest_model