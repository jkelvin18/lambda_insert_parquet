import json

import awswrangler as wr
import boto3
import pandas as pd

# Define the path to save the output data in S3
pathoutput = "s3://dlocalsotpagamentos/tb_riscos"


def extract_data(data):
    """
    Extract the relevant data from the input JSON object
    and convert it to a Pandas DataFrame

    Parameters:
    data (dict): The input JSON data

    Returns:
    df (pd.DataFrame): The extracted data in a Pandas DataFrame
    """
    new_dict = {
        "anomesdia": data["result"][0]["dataBase"],
        "codigo": data["result"][0]["atributo"][0]["atributoCodigo"],
        "tipo": data["result"][0]["atributo"][0]["atributoTipo"],
        "nome": data["result"][0]["atributo"][0]["atributoNome"],
        "valor": data["result"][0]["atributo"][0]["atributoValor"],
    }
    return pd.DataFrame.from_dict([new_dict])


def load_data(df, pathoutput):
    """
    Extract the relevant data from the input JSON object
    and convert it to a Pandas DataFrame

    Parameters:
    data (dict): The input JSON data

    Returns:
    df (pd.DataFrame): The extracted data in a Pandas DataFrame
    """
    wr.s3.to_parquet(
        df=df,
        path=pathoutput,
        table="tb_riscos",
        database="db_local",
        compression="snappy",
        dataset=True,
        partition_cols=["anomesdia"],
    )


def lambda_handler(event, context):
    """
    The main Lambda function to handle S3 events and extract and load the data

    Parameters:
    event (dict): The input event data
    context (dict): The context data

    Returns:
    str: A string indicating the success or failure of the function
    """
    s3 = boto3.client("s3")

    # Get the name of the S3 bucket and the key of the object
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key_name = event["Records"][0]["s3"]["object"]["key"]

    try:
        # Get the S3 object and load its contents as a JSON object
        json_object = s3.get_object(Bucket=bucket_name, Key=key_name)
        data = json.load(json_object["Body"])

        # Extract the data and save it to S3 as a Parquet file
        df = extract_data(data)
        load_data(df, pathoutput)

        return "Success"
    except Exception as e:
        raise e
