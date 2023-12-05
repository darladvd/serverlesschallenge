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
    pass