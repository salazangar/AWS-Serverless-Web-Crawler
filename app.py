#!/usr/bin/env python3
import os

import aws_cdk as cdk

from serverless_web_crawler.serverless_web_crawler_stack import ServerlessWebCrawlerStack


app = cdk.App()
ServerlessWebCrawlerStack(app, "ServerlessWebCrawlerStack",
   
    env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

     )

app.synth()
