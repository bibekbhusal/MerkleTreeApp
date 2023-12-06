import json
from merkle_tree import MerkleTree

# With current tree architecture and hash, the following data produces root node with this hash
demo_tree_id = "bf57020a599b6ca72c29faca759d2f5c782b0fd1b611ed529e0ea422c28daf36"
demo_data = ["7", "8", "9", "10", "11", "12", "13", "14"]


def handle_index(event, context):
    """
    Lambda Handler to handle /index API.
    :param event: Lambda event
    :param context: Lambda context
    :return: Json payload with information of the node pointed by given index.
    """

    print(f"Received {event}")

    try:
        body = json.loads(event['body'])
        tree_id = body.get('tree_id', demo_tree_id)
        index = body['index']

        print(f"Importing tree: {tree_id}")
        tree = MerkleTree.load_tree(tree_id)
        index_response = tree.index(index)
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(index_response)
        }
    except Exception as e:
        print(f"Received exception: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'text/plain'},
            'body': json.dumps(f'Unable to process request: {e}')
        }


def handle_create(event, context):
    """
    Creates a new tree based on data provided in the event body.
    :param event: Lambda event containing request data.
    :param context: Lambda context
    :return: JSON response with tree id
    """
    print(f"Received {event}")

    try:
        body = json.loads(event['body'])
        data = body.get('data', demo_data)
        print(f"Creating a new tree with data: {data}")
        tree = MerkleTree.create_new(data)

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'tree_id': tree.id})
        }
    except Exception as e:
        print(f"Received exception while loading tree: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'text/plain'},
            'body': json.dumps(f'Failed to create a tree. {e}')
        }
