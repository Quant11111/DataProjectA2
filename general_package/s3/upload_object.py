import boto3
#import datetime
import os



def upload_object(layer, group, table_name, file_name, object, **kwargs):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get("BUCKET_NAME")
    current_date = kwargs["data_interval_start"].strftime("%Y%m%d")  
    #current_date = datetime.datetime.now().strftime("%Y%m%d")
    key = f"{layer}/{group}/{table_name}/{current_date}/{file_name}"
    try:
        s3.put_object(
            Body=object,
            Bucket=bucket_name,
            Key=key
        )
        print(f"Successfully saved {key} to {bucket_name}")
    except Exception as e:
        print(f"Error occurred while saving {key} to {bucket_name}: {e}")
        raise 


"""
if kwargs.get("date")!= None:
        current_date = kwargs.get("date")
    else:
        current_date = datetime.datetime.now().strftime("%Y%m%d")
"""