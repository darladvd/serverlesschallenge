import json
import string
import random
import os
import boto3
import urllib.parse

from dynamodb_gateway import DynamodbGateway

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
queue_url = "https://sqs.us-east-1.amazonaws.com/874957933250/challenge1-dev-jobs"

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
                "customer_name": person.get("name"),
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
            "customer_name": body.get("name"),
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


def email_exists(table_name, email):
    # Check if the email already exists in the DynamoDB table using GSI
    result = DynamodbGateway.query_index_by_partition_key(
        index_name="emailIndex",
        table_name=table_name,
        partition_key_name="email",
        partition_key_query_value=email
    )

    return bool(result)
    

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
def prepare_sqs_job(event, content):
    bucket_name = os.getenv("S3_BUCKETNAME")

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    # # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # try:
    #     # Download the file from S3
    #     response = s3.get_object(Bucket=bucket, Key=key)
        
    #     # Read the file contents line by line
    #     file_content = response['Body'].read().decode('utf-8')
    #     lines = file_content.split('\n')
        
    #     # Process each line
    #     for line in lines:
    #         # Do something with each line (e.g., print it)
    #         print(line)
            
    #     # You can also return the lines or any other information as needed
    #     return {'lines': lines}
        
    # except Exception as e:
    #     print(e)
    #     print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))