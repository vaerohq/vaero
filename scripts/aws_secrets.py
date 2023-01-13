#!/usr/bin/env python
#
# Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
#
import boto3
from botocore.exceptions import ClientError
import json
import sys

# Read stdin to get secrets to retrieve
file_in = ""
for line in sys.stdin:
    file_in += line

input_arr = json.loads(file_in)

# Initialize client
region_name = "us-west-2"
session = boto3.session.Session()
client = session.client(service_name = 'secretsmanager', region_name = region_name)

# Retrieve secrets
output = {}
for secret_pair in input_arr: # iterate over all pairs
    for secret_name, arg_name in secret_pair.items(): # for each pair, get the secret and assign to argument
        try:
            get_secret_value_response = client.get_secret_value(SecretId = secret_name)
        except ClientError as e:
            raise e

        val = json.loads(get_secret_value_response["SecretString"])["token"]

        output[arg_name] = val

print(f"{json.dumps(output)}")