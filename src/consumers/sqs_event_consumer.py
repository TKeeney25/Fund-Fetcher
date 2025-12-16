import boto3
from typing import List, Dict, Any
from models.sqs_event import SQSEvent

sqs = boto3.client('sqs', region_name='us-east-1')

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/533267022303/FundFetcher-Queue'

def consume_event(max_messages: int = 1, wait_time_seconds: int = 20) -> List[Dict[str, Any]]:
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_time_seconds,
        AttributeNames=['All']
    )
    
    messages = response.get('Messages', [])
    return messages

def delete_message(receipt_handle: str) -> None:
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=receipt_handle
    )

def process_events():
    while True:
        try:
            messages = consume_event()
        except Exception as e:
            print(f"Error consuming messages: {e}")
            continue

        for message in messages:
            try:
                sqs_event = SQSEvent(**message)
                print(f"Processing message: {sqs_event.body}")
                
                # TODO: Add your business logic here
                
                delete_message(sqs_event.receipt_handle)
            except Exception as e:
                print(f"Error processing message: {e}")
                continue