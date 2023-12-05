import json
import string
import random
import os

from dynamodb_gateway import DynamodbGateway

def create_loyalty_card(event, context):
    body = json.loads(event["body"])

    print(event)

    table_name = os.getenv("DYNAMODB_CARDS_TABLE_NAME")

    loyalty_cards = []

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