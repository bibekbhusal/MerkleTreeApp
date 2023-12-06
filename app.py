#!/usr/bin/env python3

import aws_cdk as cdk

from merkle_tree_app.merkle_tree_app_stack import MerkleTreeAppStack

app = cdk.App()
MerkleTreeAppStack(app, "MerkleTreeAppStack",
                   #env=cdk.Environment(account='', region='us-west-2')
                   )
app.synth()
