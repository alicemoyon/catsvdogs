import boto3
import configparser

# Get AWS credentials from config file

config = configparser.ConfigParser()
config.read_file(open('credentials.cfg'))

AWS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
FIREHOSE_ARN = config.get('AWS', 'FIREHOSE_ROLE_ARN')

# Create an S3 bucket for the streaming data

s3 = boto3.client('s3',
                  aws_access_key_id=AWS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET,
                  region_name='replaceWithYourRegion')

s3.create_bucket(Bucket='replaceWithYourS3BucketName',
                 CreateBucketConfiguration={'LocationConstraint': 'replaceWithYourRegion'})

# Create the Firehose delivery stream

firehose = boto3.client('firehose')
res = firehose.create_delivery_stream(
    DeliveryStreamName='replaceWithYourDeliveryStreamName',
    DeliveryStreamType='DirectPut',
    S3DestinationConfiguration={
        "RoleARN": FIREHOSE_ARN,
        "BucketARN": "replaceWithYourBucketARN"
    }
)

print(res['DeliveryStreamARN'])  # to verify stream was created successfully
