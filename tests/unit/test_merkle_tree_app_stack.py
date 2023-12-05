import aws_cdk as core
import aws_cdk.assertions as assertions

from merkle_tree_app.merkle_tree_app_stack import MerkleTreeAppStack

# example tests. To run these tests, uncomment this file along with the example
# resource in merkle_tree_app/merkle_tree_app_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MerkleTreeAppStack(app, "merkle-tree-app")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
