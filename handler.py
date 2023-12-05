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
        # Extract card number from the event or query parameters
        card_number = event["headers"]["card_number"]

        table_name = os.getenv("DYNAMODB_CARDS_TABLE_NAME")

        # Query DynamoDB to get the loyalty card with the specified card number
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

    except Exception as e:
        response = {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }

    return response