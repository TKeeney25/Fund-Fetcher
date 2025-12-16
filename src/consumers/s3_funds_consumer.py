import boto3
import os
import glob

def fetch_latest_fund_file():
    s3 = boto3.client('s3')
    bucket = 'tickertrackerbucket'
    prefix = 'fundfetcher/funds/'
    
    # List all objects in the bucket path
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Delete old files in src/funds folder
    for old_file in glob.glob("src/funds/*"):
        os.remove(old_file)
        print(f"Deleted: {old_file}")
    
    if 'Contents' not in response:
        print("No objects found")
        return None
    
    # Parse object names and extract the int values
    objects = []
    for obj in response['Contents']:
        key = obj['Key']
        filename = key.split('/')[-1]
        
        # Split by underscore and remove extension
        name_parts = filename.rsplit('.', 1)[0].split('_')
        
        # Find the integer value in the parts
        for part in name_parts:
            if part.isdigit():
                objects.append((int(part), key))
                break
    
    if not objects:
        print("No valid objects found")
        return None
    
    # Find object with greatest int value
    max_obj = max(objects, key=lambda x: x[0])
    
    # Download the object to src/funds folder
    filename = max_obj[1].split('/')[-1]
    download_path = f"src/funds/{filename}"
    s3.download_file(bucket, max_obj[1], download_path)
    print(f"Downloaded: {filename}")
    
    return filename

if __name__ == "__main__":
    fetch_latest_fund_file()