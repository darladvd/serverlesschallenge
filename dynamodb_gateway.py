import boto3
import itertools
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

class DynamodbGateway:
    @classmethod
    def grouper(cls, iterable, n, fillvalue=None):
        "Collect data into fixed-length chunks or blocks"
        # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
        args = [iter(iterable)] * n
        return itertools.zip_longest(*args, fillvalue=fillvalue)

    @classmethod
    def convert_decimal_to_int(cls, item):
        for key, value in item.items():
            if isinstance(value, Decimal):
                item[key] = int(value)
        return item

    @classmethod
    def upsert(cls, table_name, mapping_data, primary_keys):
        print(f"Inserting into table {table_name}")
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)

        for group in cls.grouper(mapping_data, 100):
            batch_entries = list(filter(None.__ne__, group))

            print("=====")
            print("WRITING THIS BATCH in batches of 100")
            print(batch_entries)
            print("=====")

            with table.batch_writer(overwrite_by_pkeys=primary_keys) as batch:
                for entry in batch_entries:
                    batch.put_item(
                        Item = entry
                    )

    @classmethod
    def scan_table(cls, table_name, last_evaluated_key=None):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        items = []

        if last_evaluated_key == None:
            response = table.scan()
            items.extend(response.get('Items'))
        else:
            response = table.scan(ExclusiveStartKey=last_evaluated_key)
            items.extend(response.get('Items'))

        for item in items:
            cls.convert_decimal_to_int(item)

        if 'Items' not in response:
            raise Exception(f"There is no objects for this object on table {table_name}")

        if 'LastEvaluatedKey' not in response:
            response["LastEvaluatedKey"] = None

        while ("LastEvaluatedKey" in response) and (response["LastEvaluatedKey"] != None):
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])

            items.extend(response.get('Items'))

            print("==============================================")
            print("ITEMS FROM THE RESPONSE -- INSIDE THE LOOP")
            print(response)
            print(f"item_count: {len(items)}, LastEvaluatedKey: {response.get('LastEvaluatedKey')}")


        return {
            "items": items,
            "last_evaluated_key": response["LastEvaluatedKey"]
        }

    @classmethod
    def query_by_partition_key(cls, table_name, partition_key_name, partition_key_query_value, attributes="ALL_ATTRIBUTES"):
        print(f"Reading from table {table_name}")
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        items = []

        if attributes == "ALL_ATTRIBUTES":
            resp = table.query(
                KeyConditionExpression=Key(partition_key_name).eq(partition_key_query_value),
            )
        else:
            select_data = cls.process_projection_expression(attributes)

            resp = table.query(
                KeyConditionExpression=Key(partition_key_name).eq(partition_key_query_value),
                Select=select_data["select"],
                ProjectionExpression=select_data["expression"],
                ExpressionAttributeNames=select_data["expression_attr"]
            )

        items.extend(resp.get('Items'))

        for item in items:
            cls.convert_decimal_to_int(item)

        print("==============================================")
        print("ITEMS FROM THE RESPONSE -- BEFORE THE LOOP")
        print(resp)
        print(f"item_count: {len(items)}, LastEvaluatedKey: {resp.get('LastEvaluatedKey')}")
        print("NO LOOP ANYMORE - COMMENTED OUT")

        return {
            "items": items,
            "last_evaluated_key": resp.get('LastEvaluatedKey')
        }
    
    @classmethod
    def query_index_by_partition_key(cls, index_name, table_name, partition_key_name, partition_key_query_value):
        client = boto3.client('dynamodb')
        resp = client.query(
            TableName=table_name,
            IndexName=index_name,
            KeyConditionExpression=f"{partition_key_name} = :value",
            ExpressionAttributeValues={
                ':value': {'S': partition_key_query_value}
            }
        )

        items = resp.get('Items')

        for item in items:
            cls.convert_decimal_to_int(item)

        return items