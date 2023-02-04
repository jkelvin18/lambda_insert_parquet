
# README

## Overview
This code is a Python script that extracts data from a JSON object, loads it into a Pandas DataFrame and then stores it in an S3 bucket as a Parquet file. It is intended to be used as an AWS Lambda function.

## Prerequisites
The following packages are required to run this script:
- json
- awswrangler
- boto3
- pandas

## Usage
The `lambda_handler` function is the main entry point for the script. It takes two arguments: `event` and `context`. The event argument contains information about the S3 bucket and object key from which the data will be extracted. The context argument contains runtime information for the Lambda function. The `extract_data` function takes the JSON object as an argument and returns a Pandas DataFrame containing the extracted data. Finally, the `load_data` function takes the DataFrame and pathoutput (the S3 bucket path) as arguments and stores it in an S3 bucket as a Parquet file.