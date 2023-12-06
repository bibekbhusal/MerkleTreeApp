import time
from typing import Dict, List
import boto3
import pickle
import os

from botocore.exceptions import ClientError


class S3Client:
    s3_client = boto3.client('s3')

    @classmethod
    def save_tree(cls, tree_id: str, data: List[List[str]]) -> bool:
        """
        Save given list of hashes in the S3 bucket.
        :param tree_id: Key to use
        :param data: data to save
        :return: True if save is successful. False otherwise.
        """
        tree_bucket = os.environ["TREE_BUCKET_NAME"]
        serialized_data = pickle.dumps(data)
        try:
            cls.s3_client.put_object(Bucket=tree_bucket, Key=tree_id, Body=serialized_data)
        except ClientError as err:
            print(f"ClientError error when saving tree: {err}")
            return False
        return True

    @classmethod
    def load_tree(cls, tree_id: str) -> List[List[str]]:
        """
        Load data for the given tree_id
        :param tree_id: Key to lookup object in S3
        :return: Loaded data (which is expected to be a list of lists of string)
        """
        tree_bucket = os.environ["TREE_BUCKET_NAME"]

        try:
            response = cls.s3_client.get_object(Bucket=tree_bucket, Key=tree_id)
            data = response['Body'].read()
            return pickle.loads(data)
        except ClientError as err:
            print(f"Client error when loading tree from S3: {err}")
            return []


class DDBClient:
    ddb_client = boto3.resource('dynamodb')
    data_table_key = 'DataId'
    data_column_name = 'Data'
    max_batch_get_tries = 3
    sleep_timeout_sec = 1

    @classmethod
    def save_data(cls, data_map: Dict[str, str]) -> bool:
        """
        Save given data map (key, value) in DDB
        :param data_map: {key -> value} pair to save
        :return: True if save is successful. False otherwise.
        """
        data_table = cls.ddb_client.Table(os.environ['DATA_TABLE_NAME'])
        items = [{cls.data_table_key: hash_val, cls.data_column_name: datum} for hash_val, datum in data_map.items()]

        try:
            # Automatically handles retrying unprocessed items
            with data_table.batch_writer() as batch_writer:
                for item in items:
                    batch_writer.put_item(Item=item)
        except ClientError as err:
            print(f"Client error when loading tree from dynamodb: {err}")
            return False
        return True

    @classmethod
    def load_data(cls, data_keys: List[str]) -> Dict[str, str]:
        """
        Load data from dynamodb using given keys.
        :param data_keys: Keys of items to query
        :return: {key -> value} pairs.
        """
        data_table_name = os.environ['DATA_TABLE_NAME']

        tries = 0
        batch_keys = data_keys
        result: Dict[str, str] = {}

        # TODO: Batch the keys of they are more than 100.

        while tries < cls.max_batch_get_tries and len(batch_keys) > 0:

            keys = {
                data_table_name: {
                    "Keys": [{cls.data_table_key: hash_val} for hash_val in batch_keys]
                }
            }

            try:
                response = cls.ddb_client.batch_get_item(RequestItems=keys)
                retrieved_items = response.get('Responses', {}).get(data_table_name, [])

                print(f"Retrieved items: {retrieved_items}")

                for item in retrieved_items:
                    result[item['DataId']] = item['Data']

                batch_keys = response.get('UnprocessedKeys', [])
                print(f"Unprocessed keys: {batch_keys}")

                tries += 1

                if tries < cls.max_batch_get_tries:
                    print(f"Sleep 1 second before retrying.")
                    time.sleep(cls.sleep_timeout_sec)

            except ClientError as err:
                print(f"Client error when loading tree from dynamodb: {err}")

        return result
