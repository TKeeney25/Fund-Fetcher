import boto3
import time

def upload_fund_returns_to_s3():
    """Upload DailyFundReturns.csv to S3 with date-based filename."""
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Define paths
    file_path = 'output/DailyFundReturns.csv'
    bucket_name = 'tickertrackerbucket'
    s3_prefix = 'fundfetcher/results/'
    
    # Create filename with seconds from epoch
    epoch_seconds = int(time.time())
    s3_filename = f'DailyFundReturns_{epoch_seconds}.csv'
    s3_key = f'{s3_prefix}{s3_filename}'
    
    # Upload file
    s3_client.upload_file(
        file_path,
        bucket_name,
        s3_key
    )
    
    print(f"Successfully uploaded to s3://{bucket_name}/{s3_key}")
    return bucket_name + "/" + s3_key

if __name__ == '__main__':
    upload_fund_returns_to_s3()