import boto3
import os



def get_object(layer, group, table_name, file_name, date):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get("BUCKET_NAME")
    key = f"{layer}/{group}/{table_name}/{date}/{file_name}"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return response
    except Exception as e:
        print(f"Error occurred while getting {key} from {bucket_name}: {e}")
        raise 
    