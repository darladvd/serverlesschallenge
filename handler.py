import json
import string
import random
import os
import boto3
import urllib.parse
import csv
import sys
from io import StringIO

from dynamodb_gateway import DynamodbGateway

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
queue_url = os.getenv('QUEUE_URL')

def create_loyalty_card(event, context):
    body = json.loads(event["body"])

    print(event)

    table_name = os.getenv("DYNAMODB_CARDS_TABLE_NAME")

    loyalty_cards = []

    # Check if body is a list
    if isinstance(body, list):
        for person in body:
            email = person.get("email")

            # Check if the email already exists in the DynamoDB table
            if email_exists(table_name, email):
                response = {
                    "statusCode": 400,
                    "body": json.dumps({"status": "error", "message": "Email already used"})
                }
                return response

            # Generate a unique card number
            letters = string.ascii_lowercase
            id_var = ''.join(random.choice(letters) for _ in range(16))

            card = {
                "card_number": id_var,
                "first_name": person.get("name"),
                "email": person.get("email")
            }

            loyalty_cards.append(card)
    else:
        # If body is not a list, assume it's a single entry
        email = body.get("email")

        # Check if the email already exists in the DynamoDB table
        if email_exists(table_name, email):
            response = {
                "statusCode": 400,
                "body": json.dumps({"status": "error", "message": "Email already used"})
            }
            return response

        # Generate a unique card number
        letters = string.ascii_lowercase
        id_var = ''.join(random.choice(letters) for _ in range(16))

        card = {
            "card_number": id_var,
            "first_name": body.get("name"),
            "email": body.get("email")
        }

        loyalty_cards.append(card)

    DynamodbGateway.upsert(
        table_name=table_name,
        mapping_data=loyalty_cards,
        primary_keys=["card_number"]
    )

    response = {"statusCode": 200, "body": json.dumps(loyalty_cards)}

    return response
    

def get_all_loyalty_card(event, context):
    body = {
        "message": "I'm getting all loyalty cards",
        "input": event,
    }

    table_name = os.getenv("DYNAMODB_CARDS_TABLE_NAME")

    return_body = {}
    return_body["items"] = DynamodbGateway.scan_table(
        table_name=table_name
    )
    
    return_body["status"] = "success"

    response = {"statusCode": 200, "body": json.dumps(return_body)}

    return response


def get_one_loyalty_card(event, context):
    body = {
        "message": "I'm getting a loyalty card",
        "input": event,
    }

    try:
        # Extract card number from the event headers
        card_number = event["headers"].get("card_number")

        if card_number:
            # If card number is present, query DynamoDB to get the specific loyalty card
            table_name = os.getenv("DYNAMODB_CARDS_TABLE_NAME")
            result = DynamodbGateway.query_by_partition_key(
                table_name=table_name,
                partition_key_name="card_number",
                partition_key_query_value=card_number
            )

            if result["items"]:
                response = {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "item": result["items"][0]})
                }
            else:
                response = {
                    "statusCode": 404,
                    "body": json.dumps({"status": "error", "message": "Loyalty card not found"})
                }
        else:
            # If no card number is provided, return all loyalty cards
            return get_all_loyalty_card(event, context)

    except Exception as e:
        response = {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }

    return response

#aws lambda trigger when theres new s3 file. reads line by line
def prepare_sqs_job(event, context):
    try:
        print(f"Received S3 event: {json.dumps(event)}")

        bucket_name = os.getenv("S3_BUCKETNAME")

        # Get the object details from the S3 event
        s3_record = event['Records'][0]['s3']
        bucket = s3_record['bucket']['name']
        file_key = urllib.parse.unquote_plus(s3_record['object']['key'], encoding='utf-8')

        # Download the file from S3
        response = s3.get_object(Bucket=bucket, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        print(f"Object uploaded: s3://{bucket}/{file_key}")

        # Process CSV file and send each row as a message to SQS
        rows = [row for i, row in enumerate(csv.reader(StringIO(file_content))) if i > 0]

        message_attrs = {'AttributeName': {'StringValue': 'AttributeValue', 'DataType': 'String'}}
        for row in rows:
            print(row)

            message_body = {
                "card_number": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "email": row[3],
                "points": row[4]
            }

            try:
                res = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message_body),
                    MessageAttributes=message_attrs,
                )
                print(res)
            except Exception as e:
                print(e)

        message = 'Messages accepted!'
        print(message)
        response = {"statusCode": 200, "body": json.dumps({"status": "success", "message": message})}

    except Exception as e:
        print(f'Error: {str(e)}')
        response = {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

    return response

def process_sqs_job(event, context):
    try:
        print(f"Received SQS event: {json.dumps(event)}")

        table_name = os.getenv("DYNAMODB_CARDS_TABLE_NAME")

        for record in event['Records']:
            # Parse JSON content from SQS message
            message_body = json.loads(record['body'])

            if isinstance(message_body, dict):
                # Extract necessary information from the message
                card_number = message_body.get('card_number')
                first_name = message_body.get('first_name')
                last_name = message_body.get('last_name')
                email = message_body.get('email')
                points = message_body.get('points')

                # Check if the email already exists in the DynamoDB table
                if email_exists(table_name, email):
                    print(f"Email {email} already used. Skipping...")
                    continue

                # Create a loyalty card in DynamoDB
                loyalty_card = {
                    "card_number": card_number,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "points": points
                }

                DynamodbGateway.upsert(
                    table_name=table_name,
                    mapping_data=[loyalty_card],
                    primary_keys=["card_number"]
                )

                print(f"Loyalty card created: {loyalty_card}")

        message = 'Messages processed successfully!'
        print(message)
        response = {"statusCode": 200, "body": json.dumps({"status": "success", "message": message})}

    except Exception as e:
        print(f'Error: {str(e)}')
        response = {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

    return response


def email_exists(table_name, email):
    # Check if the email already exists in the DynamoDB table using GSI
    result = DynamodbGateway.query_index_by_partition_key(
        index_name="emailIndex",
        table_name=table_name,
        partition_key_name="email",
        partition_key_query_value=email
    )

    return bool(result)