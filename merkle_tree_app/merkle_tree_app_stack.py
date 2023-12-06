from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as _apigateway,
    aws_s3 as _s3,
    aws_dynamodb as _dynamodb
)
from constructs import Construct

LAMBDA_TIMEOUT_SEC = 15
print(f"Using lambda function timeout of: {LAMBDA_TIMEOUT_SEC}")

lambda_src_path = str(Path('.') / "source" / "src")
print(f"Lambda function src is: {lambda_src_path}")


class MerkleTreeAppStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        tree_bucket = _s3.Bucket(self, id='MerkleTreeStoreBucket', versioned=True)

        data_table = _dynamodb.Table(self,
                                     id='MerkleTreeDataTable',
                                     table_name='MerkleTreeDataTable',
                                     # TODO: Find a common place to share this among the stack and aws_client.py code.
                                     partition_key=_dynamodb.Attribute(name='DataId',
                                                                       type=_dynamodb.AttributeType.STRING))

        lambda_index = _lambda.Function(self,
                                        id='MerkleTreeIndexLambdaFunction',
                                        runtime=_lambda.Runtime.PYTHON_3_9,
                                        code=_lambda.Code.from_asset(lambda_src_path),
                                        handler='lambda_handler.handle_index',
                                        timeout=Duration.seconds(LAMBDA_TIMEOUT_SEC),
                                        environment={
                                            'TREE_BUCKET_NAME': tree_bucket.bucket_name,
                                            'DATA_TABLE_NAME': data_table.table_name
                                        })

        lambda_create = _lambda.Function(self,
                                         id='MerkleTreeCreateLambdaFunction',
                                         runtime=_lambda.Runtime.PYTHON_3_9,
                                         code=_lambda.Code.from_asset(lambda_src_path),
                                         handler='lambda_handler.handle_create',
                                         timeout=Duration.seconds(LAMBDA_TIMEOUT_SEC),
                                         environment={
                                             'TREE_BUCKET_NAME': tree_bucket.bucket_name,
                                             'DATA_TABLE_NAME': data_table.table_name
                                         })

        api = _apigateway.RestApi(self, 'MerkleTreeAppAPI', rest_api_name='merkle_app_api')

        # '/retrieve' API to fetch response about the node in the index
        index_api_integration = _apigateway.LambdaIntegration(lambda_index)
        api.root.add_resource('retrieve').add_method('POST', index_api_integration)

        # '/tree' API to create a demo tree (setting up for testing)
        create_api_integration = _apigateway.LambdaIntegration(lambda_create)
        api.root.add_resource('tree').add_method('POST', create_api_integration)

        tree_bucket.grant_read(lambda_index)
        tree_bucket.grant_write(lambda_create)

        data_table.grant_read_data(lambda_index)
        data_table.grant_write_data(lambda_create)

        # Output the API Gateway URL
        CfnOutput(
            self, 'ApiGatewayUrl',
            value=api.url,
            description='URL of the created API Gateway'
        )
